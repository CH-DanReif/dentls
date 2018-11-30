/* I can be compiled with the command "gcc -o dentls dentls.c" */

#define _GNU_SOURCE
#include <search.h>     /* Defines tree functions */
#include <dirent.h>     /* Defines DT_* constants */
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <string.h>

/* Because most filesystems use btree to store dents
 * its very important to perform an in-order removal
 * of the file contents. Performing an 'as-is read' of
 * the contents causes lots of btree rebalancing
 * that has significantly negative effect on unlink performance
 */

// Globals
unsigned long delete_count = 0; // Running count of deletions
unsigned short show_progress = 0; // By default, don't show progress
unsigned short print_only = 1; // By default, print entries, don't unlink
unsigned long progress_interval_minor = 1000; // Print a ".".
unsigned long progress_interval_major = 50; // Print the number done so far.

/* Tests indicate that performing a ascending order traversal
 * is about 1/3 faster than a descending order traversal */
int compare_fnames(const void *key1, const void *key2) {
  return strcmp((char *)key1, (char *)key2);
}

void walk_tree(const void *node, VISIT val, int lvl) {
  int rc = 0;
  switch(val) {
  case leaf:
    if (print_only) {
      printf("%s\n", *(char **)node);
    } else {
      rc = unlink(*(char **)node);
    }
    delete_count++;
    break;
  /* End order is deliberate here as it offers the best btree
   * rebalancing avoidance.
   */
  case endorder:
    if (print_only) {
      printf("%s\n", *(char **)node);
    } else {
      rc = unlink(*(char **)node);
    }
    delete_count++;
  break;
  default:
    return;
    break;
  }

  if (rc < 0) {
    fprintf(stderr, "Failed to delete %s", *(char **)node);
    perror("unlink problem");
    exit(1);
  }

  if (show_progress) {
    if (delete_count % (progress_interval_minor * progress_interval_major) == 0) {
      sleep(1);
      fprintf(stderr, "%s%lu", (delete_count ? "\n" : ""), delete_count);
      fflush(stderr);
    }
    else if (delete_count % progress_interval_minor == 0) {
      fprintf(stderr, ".");
      fflush(stderr);
    }
  }
}

void dummy_destroy(void *nil) {
  return;
}

void *tree = NULL;

struct linux_dirent {
        long           d_ino;
        off_t          d_off;
        unsigned short d_reclen;
        char           d_name[FILENAME_MAX];
        char           d_type;
};

struct linked_list_node {
  void *                    list_node_data_ptr;
  struct linked_list_node * next;
};

/* Overrides the first argument to be the new list head. */
void linked_list_prepend_override(struct linked_list_node ** head_in_out, void * data_ptr) {
  if (head_in_out == NULL) {
    fprintf(stderr, "linked_list_prepend_override: head_in_out is NULL\n");
    exit(1);
  }

  struct linked_list_node * new_head_ptr = malloc(sizeof(struct linked_list_node));
  if (!new_head_ptr) {
    perror("malloc");
    exit(1);
  }
  new_head_ptr->next = *head_in_out;
  new_head_ptr->list_node_data_ptr = data_ptr;
  *head_in_out = new_head_ptr;
}

void free_linked_list(struct linked_list_node * head) {
  while (head) {
    free(head->list_node_data_ptr);
    struct linked_list_node * next = head->next;
    free(head);
    head = next;
  }
}

int main(const int argc, const char** argv) {
    int totalfiles = 0;
    int dirfd = -1;
    int offset = 0;
    int bufcount = 0;
    void *buffer = NULL;
    char *d_type;
    struct linux_dirent *dent = NULL;
    struct stat dstat;
    struct linked_list_node * dirent_buffers_list = NULL;

    /* Test we have a directory path */
    if (argc < 2) {
        fprintf(stderr, "You must supply a valid directory path.\n");
        exit(1);
    }

    if (argv[1][0] == '-' || argv[1][0] != '/') {
      fprintf(stderr, "Usage: %s <directory>\n", argv[0]);
      fprintf(stderr, "  For safety, <directory> must be fully-qualified (i.e., start with /).\n");
      fprintf(stderr, "  Env: Set DENTLS_DELETE=delete to delete instead of print.\n");
      fprintf(stderr, "  Env: Set DENTLS_PROGRESS to show progress on stderr.\n");
      return -1;
    }

    if (getenv("DENTLS_DELETE") != NULL) {
      if (strcmp(getenv("DENTLS_DELETE"), "delete")) {
        fprintf(stderr, "If you'd like to delete files, please set DENTLS_DELETE to 'delete', *EXACTLY*.\n");
        fprintf(stderr, "If you did not intend to run deletes, please unset the variable entirely.\n");
        return 1;
      } else {
        print_only = 0;
      }
    }

    if (getenv("DENTLS_PROGRESS")) {
      show_progress = 1;
    }

    const char *path = argv[1];

    /* Standard sanity checking stuff */
    if (access(path, R_OK) < 0) {
        perror("Could not access directory");
        exit(1);
    }

    if (lstat(path, &dstat) < 0) {
        perror("Unable to lstat path");
        exit(1);
    }

    if (!S_ISDIR(dstat.st_mode)) {
        fprintf(stderr, "The path %s is not a directory.\n", path);
        exit(1);
    }

    /* We use the st_size of the directory as a rough estimation of how large
       getdents() buffers. However, we may still need multiple such buffers,
       as the actual amount of data returned by getdents() can exceed st_size.
       See https://serverfault.com/questions/183821/rm-on-a-directory-with-millions-of-files/328305#comment1148905_328305
    */
    uint64_t getdents_buf_size = dstat.st_size * 2;

    if ((buffer = malloc(getdents_buf_size)) == NULL) {
        perror("malloc failed");
        exit(1);
    }
    linked_list_prepend_override(&dirent_buffers_list, buffer);

    /* Open the directory */
    if ((dirfd = open(path, O_RDONLY)) < 0) {
        perror("Open error");
        exit(1);
    }

    /* Switch directories */
    if (fchdir(dirfd) < 0) {
      perror("fchdir");
      exit(1);
    }

    while ((bufcount = syscall(SYS_getdents, dirfd, buffer, getdents_buf_size))) {
        if (bufcount == -1) {
            perror("getdents");
            exit(1);
        }
        offset = 0;
        dent = buffer;
        while (offset < bufcount) {
            /* Dont print thisdir and parent dir */
            if (!((strcmp(".",dent->d_name) == 0) || (strcmp("..",dent->d_name) == 0))) {
                d_type = (char *)dent + dent->d_reclen-1;
                /* Only print files */
                if (*d_type == DT_REG) {
                    /* Sort all our files into a binary tree */
                    if (!tsearch(dent->d_name, &tree, compare_fnames)) {
                      fprintf(stderr, "Cannot acquire resources for tree!\n");
                      exit(1);
                    }
                    totalfiles++;
                }
            }
            offset += dent->d_reclen;
            dent = buffer + offset;
        }

        // Prepare next buffer
        if ((buffer = malloc(getdents_buf_size)) == NULL) {
            perror("malloc failed");
            exit(1);
        }
        linked_list_prepend_override(&dirent_buffers_list, buffer);
    }
    fprintf(stderr, "Total files: %d\n", totalfiles);
    fprintf(stderr, "Performing %s...\n", (print_only ? "print" : "delete"));

    twalk(tree, walk_tree);
    fprintf(stderr, "Done\n");
    close(dirfd);
    free_linked_list(dirent_buffers_list);
    tdestroy(tree, dummy_destroy);
}
