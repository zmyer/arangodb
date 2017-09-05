//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <errno.h>
#include <unistd.h>
#include <atomic>
#include <string>
#include <sys/mman.h>
#include <syslog.h>
#include "rocksdb/env.h"

// For non linux platform, the following macros are used only as place
// holder.
#if !(defined OS_LINUX) && !(defined CYGWIN) && !(defined OS_AIX)
#define POSIX_FADV_NORMAL 0     /* [MC1] no further special treatment */
#define POSIX_FADV_RANDOM 1     /* [MC1] expect random page refs */
#define POSIX_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_FADV_WILLNEED 3   /* [MC1] will need these pages */
#define POSIX_FADV_DONTNEED 4   /* [MC1] dont need these pages */
#endif

namespace rocksdb {

static Status IOError(const std::string& context, int err_number) {
  switch (err_number) {
  case ENOSPC:
    return Status::NoSpace(context, strerror(err_number));
  case ESTALE:
    return Status::IOError(Status::kStaleFile);
  default:
    return Status::IOError(context, strerror(err_number));
  }
}

class PosixHelper {
 public:
  static size_t GetUniqueIdFromFile(int fd, char* id, size_t max_size);
};

class PosixSequentialFile : public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* file, int fd,
                      const EnvOptions& options);
  virtual ~PosixSequentialFile();

  virtual Status Read(size_t n, Slice* result, char* scratch) override;
  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                                char* scratch) override;
  virtual Status Skip(uint64_t n) override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

class PosixRandomAccessFile : public RandomAccessFile {
 protected:
  std::string filename_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd,
                        const EnvOptions& options);
  virtual ~PosixRandomAccessFile();

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Prefetch(uint64_t offset, size_t n) override;

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
  virtual void Hint(AccessPattern pattern) override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

class PosixWritableFile : public WritableFile {
 protected:
  const std::string filename_;
  const bool use_direct_io_;
  int fd_;
  uint64_t filesize_;
  size_t logical_sector_size_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;
  bool fallocate_with_keep_size_;
#endif

 public:
  explicit PosixWritableFile(const std::string& fname, int fd,
                             const EnvOptions& options);
  virtual ~PosixWritableFile();

  // Need to implement this so the file is truncated correctly
  // with direct I/O
  virtual Status Truncate(uint64_t size) override;
  virtual Status Close() override;
  virtual Status Append(const Slice& data) override;
  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Fsync() override;
  virtual bool IsSyncThreadSafe() const override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual uint64_t GetFileSize() override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual Status Allocate(uint64_t offset, uint64_t len) override;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override;
#endif
#ifdef OS_LINUX
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
};

// mmap() based random-access
class PosixMmapReadableFile : public RandomAccessFile {
 private:
  int fd_;
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  PosixMmapReadableFile(const int fd, const std::string& fname, void* base,
                        size_t length, const EnvOptions& options);
  virtual ~PosixMmapReadableFile();
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

#if 1
#define gMapSize 20*1024*1024L

// ugly global used to change fadvise behaviour
#define gFadviseWillNeed false

// background routines to close and/or unmap files
void BGFileUnmapper2(void* file_info);

// data needed by background routines for close/unmap
class BGCloseInfo
{
public:
    int fd_;
    void * base_;
    size_t offset_;
    size_t length_;
    volatile uint64_t * ref_count_;
    uint64_t metadata_;

    BGCloseInfo(int fd, void * base, size_t offset, size_t length,
                volatile uint64_t * ref_count, uint64_t metadata)
        : fd_(fd), base_(base), offset_(offset), length_(length),
          ref_count_(ref_count), metadata_(metadata)
    {
        // reference count of independent file object count
        if (NULL!=ref_count_)
          __sync_add_and_fetch(ref_count_, 1);

        // reference count of threads/paths using this object
        //  (because there is a direct path and a threaded path usage)
//        RefInc();
    };

    virtual ~BGCloseInfo() {};

    virtual void operator()() {BGFileUnmapper2(this);};

private:
    BGCloseInfo();
    BGCloseInfo(const BGCloseInfo &);
    BGCloseInfo & operator=(const BGCloseInfo &);

};

class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
  uint64_t metadata_offset_; // Offset where sst metadata starts, or zero
  bool pending_sync_;     // Have we done an munmap of unsynced data?
  bool is_async_;        // can this file process in background
  volatile uint64_t * ref_count_; // alternative to std:shared_ptr that is thread safe everywhere

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  bool UnmapCurrentRegion() {
    bool result = true;
    if (base_ != NULL) {
      if (last_sync_ < limit_) {
        // Defer syncing this data until next Sync() call, if any
        pending_sync_ = true;
      }


      // write only files can perform operations async, but not
      //  files that might re-open and read again soon
      if (!is_async_)
      {
          BGCloseInfo * ptr=new BGCloseInfo(fd_, base_, file_offset_, limit_-base_,
                                            NULL, metadata_offset_);
          BGFileUnmapper2(ptr);
      }   // if

      // called from user thread, move these operations to background
      //  queue
      else
      {
          BGCloseInfo * ptr=new BGCloseInfo(fd_, base_, file_offset_, limit_-base_,
                                            ref_count_, metadata_offset_);
          Env::Default()->Schedule(&BGFileUnmapper2, ptr, Env::Priority::HIGH);
            //gWriteThreads->Submit(ptr);
      }   // else

      file_offset_ += limit_ - base_;
      base_ = NULL;
      limit_ = NULL;
      last_sync_ = NULL;
      dst_ = NULL;

    }

    return result;
  }

  bool MapNewRegion() {
    size_t offset_adjust;

    // append mode file might not have file_offset_ on a page boundry
    offset_adjust=file_offset_ % page_size_;
    if (0!=offset_adjust)
        file_offset_-=offset_adjust;

    assert(base_ == NULL);
    if (ftruncate(fd_, file_offset_ + map_size_) < 0) {
      return false;
    }
    void* ptr = mmap(NULL, map_size_, PROT_WRITE, MAP_SHARED,
                     fd_, file_offset_);
    if (ptr == MAP_FAILED) {
      return false;
    }
    base_ = reinterpret_cast<char*>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_ + offset_adjust;
    last_sync_ = base_;
    return true;
  }

 public:
  PosixMmapFile(const std::string& fname, int fd,
                size_t page_size, const EnvOptions & opts, size_t file_offset=0L,
                bool is_async=false,
                size_t map_size=gMapSize)
      : filename_(fname),
        fd_(fd),
        page_size_(page_size),
        map_size_(Roundup(map_size, page_size)),
        base_(NULL),
        limit_(NULL),
        dst_(NULL),
        last_sync_(NULL),
        file_offset_(file_offset),
        metadata_offset_(0),
        pending_sync_(false),
        is_async_(is_async),
        ref_count_(NULL)
    {
    assert((page_size & (page_size - 1)) == 0);

    if (is_async_)
    {
        ref_count_=new volatile uint64_t[2];
        *ref_count_=1;      // one ref count for PosixMmapFile object
        *(ref_count_+1)=0;  // filesize
    }   // if

    // when global set, make entire file use FADV_WILLNEED
    if (gFadviseWillNeed)
        metadata_offset_=1;

//    gPerfCounters->Inc(ePerfRWFileOpen);
  }

  ~PosixMmapFile() {
    if (fd_ >= 0) {
      PosixMmapFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t left = data.size();
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        if (!UnmapCurrentRegion() ||
            !MapNewRegion()) {
          return IOError(filename_, errno);
        }
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    size_t file_length;
    int ret_val;


    // compute actual file length before final unmap
    file_length=file_offset_ + (dst_ - base_);

    if (!UnmapCurrentRegion()) {
        s = IOError(filename_, errno);
    }

    // hard code
    if (!is_async_)
    {
        ret_val=ftruncate(fd_, file_length);
        if (0!=ret_val)
        {
            syslog(LOG_ERR,"Close ftruncate failed [%d, %m]", errno);
            s = IOError(filename_, errno);
        }   // if

        ret_val=close(fd_);
    }  // if

    // async close
    else
    {
        *(ref_count_ +1)=file_length;
        ret_val=ReleaseRef(ref_count_, fd_);

        // retry once if failed
        if (0!=ret_val)
        {
            Env::Default()->SleepForMicroseconds(500000);
            ret_val=ReleaseRef(ref_count_, fd_);
            if (0!=ret_val)
            {
                syslog(LOG_ERR,"ReleaseRef failed in Close");
                s = IOError(filename_, errno);
                delete [] ref_count_;

                // force close
                ret_val=close(fd_);
            }   // if
        }   // if
    }   // else

    fd_ = -1;
    ref_count_=NULL;
    base_ = NULL;
    limit_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      if (fdatasync(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (dst_ > last_sync_) {
      // Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
    }

    return s;
  }

  virtual void SetMetadataOffset(uint64_t Metadata)
  {
      // when global set, make entire file use FADV_WILLNEED,
      //  so ignore this setting
      if (!gFadviseWillNeed && 1!=metadata_offset_)
          metadata_offset_=Metadata;
  }   // SetMetadataOffset


  // if std::shared_ptr was guaranteed thread safe everywhere
  //  the following function would be best written differently
  static int ReleaseRef(volatile uint64_t * Count, int File)
  {
      bool good;

      good=true;
      if (NULL!=Count)
      {
          int ret_val;

          ret_val=__sync_sub_and_fetch(Count, 1);
          if (0==ret_val)
          {
              ret_val=ftruncate(File, *(Count +1));
              if (0!=ret_val)
              {
                  syslog(LOG_ERR,"ReleaseRef ftruncate failed [%d, %m]", errno);
//                  gPerfCounters->Inc(ePerfBGWriteError);
                  good=false;
              }   // if

              if (good)
              {
                  ret_val=close(File);
                  if (0==ret_val)
                  {
//                      gPerfCounters->Inc(ePerfRWFileClose);
                  }   // if
                  else
                  {
                      syslog(LOG_ERR,"ReleaseRef close failed [%d, %m]", errno);
//                      gPerfCounters->Inc(ePerfBGWriteError);
                      good=false;
                  }   // else

              }   // if

              if (good)
                  delete [] Count;
              else
                __sync_add_and_fetch(Count, 1); // try again.

          }   // if
      }   // if

      return(good ? 0 : -1);

  }   // static ReleaseRef

};
#else
class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;  // If false, fallocate calls are bypassed
  bool fallocate_with_keep_size_;
#endif

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  Status MapNewRegion();
  Status UnmapCurrentRegion();
  Status Msync();

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                const EnvOptions& options);
  ~PosixMmapFile();

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual Status Truncate(uint64_t size) override { return Status::OK(); }
  virtual Status Close() override;
  virtual Status Append(const Slice& data) override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Fsync() override;
  virtual uint64_t GetFileSize() override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual Status Allocate(uint64_t offset, uint64_t len) override;
#endif
};
#endif
class PosixRandomRWFile : public RandomRWFile {
 public:
  explicit PosixRandomRWFile(const std::string& fname, int fd,
                             const EnvOptions& options);
  virtual ~PosixRandomRWFile();

  virtual Status Write(uint64_t offset, const Slice& data) override;

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Fsync() override;
  virtual Status Close() override;

 private:
  const std::string filename_;
  int fd_;
};

class PosixDirectory : public Directory {
 public:
  explicit PosixDirectory(int fd) : fd_(fd) {}
  ~PosixDirectory();
  virtual Status Fsync() override;

 private:
  int fd_;
};

}  // namespace rocksdb
