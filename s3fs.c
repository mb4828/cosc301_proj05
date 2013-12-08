/* This code is based on the fine code written by Joseph Pfeiffer for his
   fuse system tutorial. */

#include "s3fs.h"
#include "libs3_wrapper.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>

#define GET_PRIVATE_DATA ((s3context_t *) fuse_get_context()->private_data)

/*
 * Expand an existing directory to make room 
 * for more entries
 */
s3dirent_t *expandarray(s3dirent_t *old) {
	// get size of array
	int size = sizeof(*old)/sizeof(s3dirent_t);
	
	// create new array that's twice as large
	s3dirent_t *new = (s3dirent_t*) malloc(sizeof(s3dirent_t)*size*2);

	// copy directory objects from old array to new array
	int i=0;
	for (; i<size; i++) {
		new[i].type = old[i].type;
		strncpy(new[i].name, old[i].name, 255);

		new[i].mode = old[i].mode;
		new[i].nlink = old[i].nlink;
		new[i].uid = old[i].uid;
		new[i].gid = old[i].gid;
		new[i].size = old[i].size;
		new[i].atime = old[i].atime;
		new[i].mtime = old[i].mtime;
		new[i].ctime = old[i].ctime;
	}

	// zero out remaining entries
	size *= 2;
	for (; i<size; i++) {
		strncpy(new[i].name, "\0", 255);
	}

	return new;
}

/* *************************************** */
/*        Stage 1 callbacks                */
/* *************************************** */

/*
 * Initialize the file system.  This is called once upon
 * file system startup.
 */
void *fs_init(struct fuse_conn_info *conn)
{
    fprintf(stderr, "fs_init --- initializing file system.\n");
	s3context_t *ctx = GET_PRIVATE_DATA;

	// clear bucket
	fprintf(stderr, "  clearing bucket\n");
	char *s3bucket = getenv(S3BUCKET);
	s3fs_clear_bucket(s3bucket);
	
	// create root directory array
	fprintf(stderr, "  creating root array\n");
	s3dirent_t root[8];
	
	// set root directory entry
	root[0].type = 'd';
	strncpy(root[0].name, ".", 255);

	root[0].mode = (S_IFDIR | S_IRWXU | S_IRWXG | S_IRWXO); // directory, 777
	root[0].nlink = 1;
	root[0].uid = 0;
	root[0].gid = 0;
	root[0].size = sizeof(s3dirent_t);

	time_t now = time(NULL);
	root[0].atime = now;
	root[0].mtime = now;
	root[0].ctime = now;

	// zero out remaining root object names
	int i=1;
	for (; i<8; i++) {
		strncpy(&root[i].name, "\0", 255);
	}
	
	// store root array
	fprintf(stderr, "  storing root array\n");
	ssize_t size = sizeof(root);

	ssize_t rv = s3fs_put_object(s3bucket, "/", (uint8_t*)&root, size);
    if (rv < 0) {
        fprintf(stderr, "  failure in s3fs_put_object\n");
    } else if (rv < size) {
        fprintf(stderr, "  failed to upload full root object (s3fs_put_object %ld)\n", rv);
    } else {
        fprintf(stderr, "  successfully put root array in s3 (s3fs_put_object)\n");
    }
	
	// finish up
	fprintf(stderr, "  file system initialized\n");
    return ctx;
}

/*
 * Clean up filesystem -- free any allocated data.
 * Called once on filesystem exit.
 */
void fs_destroy(void *userdata) {
    fprintf(stderr, "fs_destroy --- shutting down file system.\n");
    free(userdata);
}


/* 
 * Get file attributes.  Similar to the stat() call
 * (and uses the same structure).  The st_dev, st_blksize,
 * and st_ino fields are ignored in the struct (and 
 * do not need to be filled in).
 */
int fs_getattr(const char *path, struct stat *statbuf) {
    fprintf(stderr, "fs_getattr(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

	if (path==NULL) {
		fprintf(stderr, "  no path name provided; aborting\n");
		return -ENOENT;
	}

	// break up path into key and basename
	const char *key = dirname((char*)path);
	const char *name = basename((char*)path);

	if ( strcmp(key, ".")==0 ) {
		fprintf(stderr, "  relative key name not supported by fs_getattr: %s\n", key);
		return -EIO;
	}
		
	// retrieve array object
	fprintf(stderr, "  retrieving object from s3\n");	
	char *s3bucket = getenv(S3BUCKET);
	s3dirent_t *dir;

	ssize_t rv = s3fs_get_object(s3bucket, key, (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
        fprintf(stderr, "  key does not exist in s3: %s\n", key);
		return -ENOENT;
    } else if (rv < sizeof(dir)) {
        fprintf(stderr, "  failed to retrieve entire object (s3fs_get_object %ld)\n", rv);
		return -ENOENT;
    }

	// does name exist in array?
	int i=0;
	if ( strcmp(name, "/")!=0 ) {
		int size = sizeof(dir)/sizeof(s3dirent_t);
		for (; i<size; i++) {
			if ( strcmp(dir[i].name, name)==0 )
				break;
		}
		
		if (i>=size) {
			fprintf(stderr, "  name does not exist in array: %s\n",name);
			return -ENOENT;
		}
	}

	// is name a directory or file? handle accordingly 
	if ( dir[i].type=='d' ) {
		fprintf(stderr, "  %s is a directory\n",name);

		// open directory containing the needed metadata
		free(dir);
		rv = s3fs_get_object(s3bucket, path, (uint8_t**)&dir, 0, 0);
    	if (rv < 0) {
        	fprintf(stderr, "  directory does not exist in s3\n");
			return -ENOENT;
    	} else if (rv < sizeof(dir)) {
        	fprintf(stderr, "  failed to retrieve entire object (s3fs_get_object %ld)\n", rv);
			return -ENOENT;
    	}

		// set i to 0 so we look at the "." entry
		i=0;
	}
	else if ( dir[i].type=='f' ) {
		fprintf(stderr, "  %s is a file\n",name);
		// do nothing because we already have the location of our metadata
	}
	else {
		fprintf(stderr, "  %s is an unknown file type; aborting\n",name);
		return -EIO;
	}

	// set statbuf with metadata from retrieved object; i is the index of the s3dirent_t containing data
	/* skip st_dev */
	/* skip st_ino */
	statbuf->st_mode = dir[i].mode;
	statbuf->st_nlink = dir[i].nlink;
	statbuf->st_uid = dir[i].uid;
	statbuf->st_gid = dir[i].gid;
	statbuf->st_rdev = 0;
	statbuf->st_size = dir[i].size;
	/* skip st_blksize */
	statbuf->st_blocks = (dir[i].size)/512;
	statbuf->st_atime = dir[i].atime;
	statbuf->st_mtime = dir[i].mtime;
	statbuf->st_ctime = dir[i].ctime;

	// finish up
	free(dir);
    return 0;
}


/*
 * Open directory
 *
 * This method should check if the open operation is permitted for
 * this directory
 */
int fs_opendir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_opendir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

	char *s3bucket = getenv(S3BUCKET);
	s3dirent_t *dir;

	// retrieve array object
	fprintf(stderr, "  retrieving object from s3\n");
	ssize_t rv = s3fs_get_object(s3bucket, path, (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
       	fprintf(stderr, "  object does not exist in s3 or is not a directory\n");
		return -ENOENT;
    } else if (rv < sizeof(dir)) {
       	fprintf(stderr, "  failed to retrieve entire object (s3fs_get_object %ld)\n", rv);
		return -ENOENT;
    } else {
       	fprintf(stderr, "  successfully retrieved array object from s3 (s3fs_get_object)\n");
    }

	// verify that it is a directory
	if (dir[0].type != 'd') {
		fprintf(stderr, "  object is not a directory\n");
		return -ENOTDIR;
	}

	// finish up
	free(dir);
    return 0;
}


/*
 * Read directory.  See the project description for how to use the filler
 * function for filling in directory items.
 */
int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_readdir(path=\"%s\", buf=%p, offset=%d)\n", path, buf, (int)offset);
	s3context_t *ctx = GET_PRIVATE_DATA;

	char *s3bucket = getenv(S3BUCKET);
	s3dirent_t *dir;

	// retrieve array object
	fprintf(stderr, "  retrieving directory array from s3\n");
	ssize_t rv = s3fs_get_object(s3bucket, path, (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
       	fprintf(stderr, "  array retrieval failed\n");
		return -ENOENT;
    } else if (rv < sizeof(dir)) {
       	fprintf(stderr, "  failed to retrieve entire object (s3fs_get_object %ld)\n", rv);
		return -ENOENT;
    } else {
       	fprintf(stderr, "  successfully retrieved array object from s3 (s3fs_get_object)\n");
    }

	// use filler to fill directory entries to supplied buffer
	int numdirent = sizeof(dir) / sizeof(s3dirent_t);
	int i=0;

	for (; i < numdirent; i++) {
		if ( strcmp(dir[i].name,"\0")!=0 ) {
			if (filler(buf, dir[i].name, NULL, 0) != 0) {
				return -ENOMEM;
			}
		}
	}

	// finish up
	free(dir);
    return 0;
}


/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_releasedir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}


/* 
 * Create a new directory.
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits (for setting in the metadata)
 * use mode|S_IFDIR.
 */
int fs_mkdir(const char *path, mode_t mode) {
    fprintf(stderr, "fs_mkdir(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    mode |= S_IFDIR;

	char *s3bucket = getenv(S3BUCKET);
	s3dirent_t *dir;

	// does this directory already exist?
	fprintf(stderr, "  checking if directory already exists in s3\n");
	ssize_t rv = s3fs_get_object(s3bucket, path, (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
       	fprintf(stderr, "  path %s is a new directory\n", path);
    } else {
       	fprintf(stderr, "  path %s already exists!\n", path);
		return -EEXIST;
    }

	// open parent directory
	free(dir);
	rv = s3fs_get_object(s3bucket, dirname((char*)path), (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
       	fprintf(stderr, "  cannot open parent directory: %s\n", dirname((char*)path) );
		return -ENOENT;
    } else if (rv != sizeof(dir)) {
       	fprintf(stderr, "  failed to retrieve entire parent (s3fs_get_object %ld)\n", rv);
		return -ENOENT;
    }


	// create a new directory array
	fprintf(stderr, "  creating directory\n");
	s3dirent_t temp[8];
	time_t now = time(NULL);
	
	// set . entry
	temp[0].type = 'd';
	strncpy(temp[0].name, ".", 255);

	temp[0].mode = mode;
	temp[0].nlink = 1;
	temp[0].uid = 0;
	temp[0].gid = 0;
	temp[0].size = sizeof(temp);
	temp[0].atime = now;
	temp[0].mtime = now;
	temp[0].ctime = now;

	// set .. entry - don't store any metadata here
	temp[1].type = 'd';
	strncpy(temp[1].name, "..", 255);

	// zero out remaining array object names
	int i=2;
	for (; i<8; i++) {
		strncpy(&temp[i].name, "\0", 255);
	}

	
	// update parent directory - don't store any metadata here
	int size = sizeof(dir)/sizeof(s3dirent_t);
	i=1;
	for (; i < size; i++ ) {
		if ( strcmp(dir[i].name, "\0")==0 )
			break;
	}

	if ( i >= size ) {
		// need to expand the parent array to make room
		dir = expandarray(dir);
	}

	dir[i].type = 'd';
	strncpy(dir[i].name, basename((char*)path), 255);

	// replace parent directory on s3
	fprintf(stderr, "  adding new directory and updated parent to s3\n");
    if (s3fs_remove_object(s3bucket, dirname((char*)path)) < 0) {
        fprintf(stderr, "  failure to remove old parent from s3 (s3fs_remove_object)\n");
		return -EIO;
    }

	size = sizeof(dir);
	rv = s3fs_put_object(s3bucket, dirname((char*)path), (uint8_t*)dir, size);
	if (rv < 0) {
    	fprintf(stderr, "  failure to put new parent on s3\n");
		return -EIO;
    } else if (rv < size) {
       	fprintf(stderr, "  failed to upload full new parent (s3fs_put_object %ld)\n", rv);
		return -EIO;
    }

	// add new directory to s3
	size = sizeof(temp);
	rv = s3fs_put_object(s3bucket, path, (uint8_t*)&temp, size);
    if (rv < 0) {
        fprintf(stderr, "  failure to put new directory on s3\n");
		return -EIO;
    } else if (rv < size) {
        fprintf(stderr, "  failed to upload full root object (s3fs_put_object %ld)\n", rv);
		return -EIO;
    }

	fprintf(stderr, "  successfully updated s3\n");

	// finish up
	free(dir);
    return 0;
}


/*
 * Remove a directory. 
 */
int fs_rmdir(const char *path) {
    fprintf(stderr, "fs_rmdir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

	char *s3bucket = getenv(S3BUCKET);
	const char *parentkey = dirname((char*)path);
	const char *name = basename((char*)path);
	s3dirent_t *dir;

	// open directory to be removed
	fprintf(stderr, "  retrieving directory from s3\n");
	ssize_t rv = s3fs_get_object(s3bucket, path, (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
       	fprintf(stderr, "  array retrieval failed\n");
		return -ENOENT;
    } else if (rv < sizeof(dir)) {
       	fprintf(stderr, "  failed to retrieve entire object (s3fs_get_object %ld)\n", rv);
		return -ENOENT;
    }

	// double check that it is a directory
	if (dir[0].type != 'd') {
		fprintf(stderr, "  object is not a directory\n");
		return -ENOTDIR;
	}

	// verify that directory is empty except for . and .. entries
	int dirsize = sizeof(dir)/sizeof(s3dirent_t);
	int i=0;
	for (; i<dirsize; i++) {
		if ( strcmp(dir[i].name, "\0")!=0 && ( strcmp(dir[i].name, ".")!=0 || strcmp(dir[i].name, "..")!=0 ) ) {
			fprintf(stderr, "  directory is not empty: %s", dir[i].name);
			return -ENOTEMPTY;
		}
	}	


	// open parent directory
	fprintf(stderr, "  updating parent directory\n");
	free(dir);
	rv = s3fs_get_object(s3bucket, parentkey, (uint8_t**)&dir, 0, 0);
    if (rv < 0) {
       	fprintf(stderr, "  cannot open parent directory: %s\n", parentkey );
		return -ENOENT;
    } else if (rv != sizeof(dir)) {
       	fprintf(stderr, "  failed to retrieve entire parent (s3fs_get_object %ld)\n", rv);
		return -ENOENT;
    }

	// find entry in parent for directory to be removed and zero out
	dirsize = sizeof(dir)/sizeof(s3dirent_t);
	for (i=0; i<dirsize; i++) {
		if (strcmp(dir[i].name, name)==0) {
			strncpy(dir[i].name, "\0", 255);
			break;
		}
	}

	if (i>=dirsize) {
		fprintf(stderr, "  could not find entry in parent\n");
		return -ENOENT;
	}

	// store updated parent on s3
    if (s3fs_remove_object(s3bucket, parentkey) < 0) {
        fprintf(stderr, "  failure to remove old parent from s3 (s3fs_remove_object)\n");
		return -EIO;
    }	

	dirsize = sizeof(dir);
	rv = s3fs_put_object(s3bucket, parentkey, (uint8_t*)&dir, dirsize);
	if (rv < 0) {
    	fprintf(stderr, "  failure to put new parent on s3\n");
		return -EIO;
    } else if (rv < dirsize) {
       	fprintf(stderr, "  failed to upload full new parent (s3fs_put_object %ld)\n", rv);
		return -EIO;
    }


	// remove directory from s3
	fprintf(stderr, "  removing directory from s3\n");
	if (s3fs_remove_object(s3bucket, path) < 0) {
        fprintf(stderr, "  failure to remove directory from s3 (s3fs_remove_object)\n");
		return -EIO;
    }	

	// finish up
	free(dir);
    return 0;
}


/* *************************************** */
/*        Stage 2 callbacks                */
/* *************************************** */


/* 
 * Create a file "node".  When a new file is created, this
 * function will get called.  
 * This is called for creation of all non-directory, non-symlink
 * nodes.  You *only* need to handle creation of regular
 * files here.  (See the man page for mknod (2).)
 */
int fs_mknod(const char *path, mode_t mode, dev_t dev) {
    fprintf(stderr, "fs_mknod(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/* 
 * File open operation
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  
 * 
 * Optionally open may also return an arbitrary filehandle in the 
 * fuse_file_info structure (fi->fh).
 * which will be passed to all file operations.
 * (In stages 1 and 2, you are advised to keep this function very,
 * very simple.)
 */
int fs_open(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_open(path\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/* 
 * Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  
 */
int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_read(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
          path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.
 */
int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_write(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
          path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.  
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 */
int fs_release(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_release(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath) {
    fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Remove a file.
 */
int fs_unlink(const char *path) {
    fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}
/*
 * Change the size of a file.
 */
int fs_truncate(const char *path, off_t newsize) {
    fprintf(stderr, "fs_truncate(path=\"%s\", newsize=%d)\n", path, (int)newsize);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Change the size of an open file.  Very similar to fs_truncate (and,
 * depending on your implementation), you could possibly treat it the
 * same as fs_truncate.
 */
int fs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_ftruncate(path=\"%s\", offset=%d)\n", path, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Check file access permissions.  For now, just return 0 (success!)
 * Later, actually check permissions (don't bother initially).
 */
int fs_access(const char *path, int mask) {
    fprintf(stderr, "fs_access(path=\"%s\", mask=0%o)\n", path, mask);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}


/*
 * The struct that contains pointers to all our callback
 * functions.  Those that are currently NULL aren't 
 * intended to be implemented in this project.
 */
struct fuse_operations s3fs_ops = {
  .getattr     = fs_getattr,    // get file attributes
  .readlink    = NULL,          // read a symbolic link
  .getdir      = NULL,          // deprecated function
  .mknod       = fs_mknod,      // create a file
  .mkdir       = fs_mkdir,      // create a directory
  .unlink      = fs_unlink,     // remove/unlink a file
  .rmdir       = fs_rmdir,      // remove a directory
  .symlink     = NULL,          // create a symbolic link
  .rename      = fs_rename,     // rename a file
  .link        = NULL,          // we don't support hard links
  .chmod       = NULL,          // change mode bits: not implemented
  .chown       = NULL,          // change ownership: not implemented
  .truncate    = fs_truncate,   // truncate a file's size
  .utime       = NULL,          // update stat times for a file: not implemented
  .open        = fs_open,       // open a file
  .read        = fs_read,       // read contents from an open file
  .write       = fs_write,      // write contents to an open file
  .statfs      = NULL,          // file sys stat: not implemented
  .flush       = NULL,          // flush file to stable storage: not implemented
  .release     = fs_release,    // release/close file
  .fsync       = NULL,          // sync file to disk: not implemented
  .setxattr    = NULL,          // not implemented
  .getxattr    = NULL,          // not implemented
  .listxattr   = NULL,          // not implemented
  .removexattr = NULL,          // not implemented
  .opendir     = fs_opendir,    // open directory entry
  .readdir     = fs_readdir,    // read directory entry
  .releasedir  = fs_releasedir, // release/close directory
  .fsyncdir    = NULL,          // sync dirent to disk: not implemented
  .init        = fs_init,       // initialize filesystem
  .destroy     = fs_destroy,    // cleanup/destroy filesystem
  .access      = fs_access,     // check access permissions for a file
  .create      = NULL,          // not implemented
  .ftruncate   = fs_ftruncate,  // truncate the file
  .fgetattr    = NULL           // not implemented
};



/* 
 * You shouldn't need to change anything here.  If you need to
 * add more items to the filesystem context object (which currently
 * only has the S3 bucket name), you might want to initialize that
 * here (but you could also reasonably do that in fs_init).
 */
int main(int argc, char *argv[]) {
    // don't allow anything to continue if we're running as root.  bad stuff.
    if ((getuid() == 0) || (geteuid() == 0)) {
    	fprintf(stderr, "Don't run this as root.\n");
    	return -1;
    }
    s3context_t *stateinfo = malloc(sizeof(s3context_t));
    memset(stateinfo, 0, sizeof(s3context_t));

    char *s3key = getenv(S3ACCESSKEY);
    if (!s3key) {
        fprintf(stderr, "%s environment variable must be defined\n", S3ACCESSKEY);
        return -1;
    }
    char *s3secret = getenv(S3SECRETKEY);
    if (!s3secret) {
        fprintf(stderr, "%s environment variable must be defined\n", S3SECRETKEY);
        return -1;
    }
    char *s3bucket = getenv(S3BUCKET);
    if (!s3bucket) {
        fprintf(stderr, "%s environment variable must be defined\n", S3BUCKET);
        return -1;
    }
    strncpy((*stateinfo).s3bucket, s3bucket, BUFFERSIZE);

    fprintf(stderr, "Initializing s3 credentials\n");
    s3fs_init_credentials(s3key, s3secret);

    fprintf(stderr, "Totally clearing s3 bucket\n");
    s3fs_clear_bucket(s3bucket);

    fprintf(stderr, "Starting up FUSE file system.\n");
    int fuse_stat = fuse_main(argc, argv, &s3fs_ops, stateinfo);
    fprintf(stderr, "Startup function (fuse_main) returned %d\n", fuse_stat);
    
    return fuse_stat;
}
