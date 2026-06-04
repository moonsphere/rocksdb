// Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr size_t kLZ4StreamingChunkSize = 16 * 1024;
constexpr size_t kLZ4StreamingMinCompressionSize = 64;

enum class VarintParseResult {
  kDone,
  kIncomplete,
  kError,
};

VarintParseResult ParseVarint32FromString(const std::string& input,
                                          size_t* offset, uint32_t* value) {
  assert(offset != nullptr);
  assert(value != nullptr);
  assert(*offset <= input.size());
  const char* start = input.data() + *offset;
  const char* limit = input.data() + input.size();
  const char* end = GetVarint32Ptr(start, limit, value);
  if (end != nullptr) {
    *offset = static_cast<size_t>(end - input.data());
    return VarintParseResult::kDone;
  }
  return input.size() - *offset < 5 ? VarintParseResult::kIncomplete
                                    : VarintParseResult::kError;
}

}  // namespace

StreamingCompress* StreamingCompress::Create(CompressionType compression_type,
                                             const CompressionOptions& opts,
                                             uint32_t compress_format_version,
                                             size_t max_output_len) {
  switch (compression_type) {
    case kLZ4Compression: {
      if (!LZ4_Streaming_Supported()) {
        return nullptr;
      }
      return new LZ4StreamingCompress(opts, compress_format_version,
                                      max_output_len);
    }
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return new ZSTDStreamingCompress(opts, compress_format_version,
                                       max_output_len);
    }
    default:
      return nullptr;
  }
}

StreamingUncompress* StreamingUncompress::Create(
    CompressionType compression_type, uint32_t compress_format_version,
    size_t max_output_len) {
  switch (compression_type) {
    case kLZ4Compression: {
      if (!LZ4_Streaming_Supported()) {
        return nullptr;
      }
      return new LZ4StreamingUncompress(compress_format_version,
                                        max_output_len);
    }
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return new ZSTDStreamingUncompress(compress_format_version,
                                         max_output_len);
    }
    default:
      return nullptr;
  }
}

bool LZ4StreamingCompress::BuildCompressedOutput(const char* input,
                                                 size_t input_size) {
  assert(input != nullptr);
  if (input_size > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  compressed_output_.clear();
  output_offset_ = 0;
  if (input_size == 0) {
    return true;
  }
#ifndef LZ4
  (void)input;
  (void)input_size;
  return false;
#else
  size_t input_offset = 0;
  while (input_offset < input_size) {
    const size_t chunk_size =
        std::min(kLZ4StreamingChunkSize, input_size - input_offset);
    if (chunk_size < kLZ4StreamingMinCompressionSize) {
      PutVarint32(&compressed_output_, static_cast<uint32_t>(chunk_size));
      PutVarint32(&compressed_output_, 0);
      compressed_output_.append(input + input_offset, chunk_size);
      input_offset += chunk_size;
      continue;
    }
    int compression_bound =
        LZ4_compressBound(static_cast<int>(chunk_size));
    if (compression_bound <= 0) {
      return false;
    }
    std::string compressed;
    compressed.resize(static_cast<size_t>(compression_bound));
    int compressed_size = 0;
#if LZ4_VERSION_NUMBER >= 10700  // r129+
    int acceleration = opts_.level < 0 ? -opts_.level : 1;
    compressed_size = LZ4_compress_fast(
        input + input_offset, &compressed[0], static_cast<int>(chunk_size),
        compression_bound, acceleration);
#else
    compressed_size = LZ4_compress_limitedOutput(
        input + input_offset, &compressed[0], static_cast<int>(chunk_size),
        compression_bound);
#endif
    if (compressed_size <= 0) {
      return false;
    }
    PutVarint32(&compressed_output_, static_cast<uint32_t>(chunk_size));
    if (static_cast<size_t>(compressed_size) >= chunk_size) {
      PutVarint32(&compressed_output_, 0);
      compressed_output_.append(input + input_offset, chunk_size);
    } else {
      PutVarint32(&compressed_output_, static_cast<uint32_t>(compressed_size));
      compressed_output_.append(compressed.data(),
                                static_cast<size_t>(compressed_size));
    }
    input_offset += chunk_size;
  }
  return true;
#endif
}

int LZ4StreamingCompress::Compress(const char* input, size_t input_size,
                                   char* output, size_t* output_pos) {
  assert(input != nullptr && output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  if (input_size == 0) {
    return 0;
  }
  if (compressed_output_.empty() && output_offset_ == 0 &&
      !BuildCompressedOutput(input, input_size)) {
    Reset();
    return -1;
  }
  assert(output_offset_ <= compressed_output_.size());
  const size_t remaining = compressed_output_.size() - output_offset_;
  const size_t output_size = std::min(max_output_len_, remaining);
  if (output_size > 0) {
    std::memcpy(output, compressed_output_.data() + output_offset_,
                output_size);
    output_offset_ += output_size;
    *output_pos = output_size;
  }
  const size_t output_remaining = compressed_output_.size() - output_offset_;
  return static_cast<int>(
      std::min(output_remaining,
               static_cast<size_t>(std::numeric_limits<int>::max())));
}

void LZ4StreamingCompress::Reset() {
  compressed_output_.clear();
  output_offset_ = 0;
}

LZ4StreamingUncompress::DecodeResult LZ4StreamingUncompress::DecodeNextChunk() {
  size_t offset = 0;
  uint32_t uncompressed_size = 0;
  uint32_t compressed_size = 0;
  VarintParseResult parse_result =
      ParseVarint32FromString(pending_input_, &offset, &uncompressed_size);
  if (parse_result == VarintParseResult::kIncomplete) {
    return DecodeResult::kIncomplete;
  }
  if (parse_result == VarintParseResult::kError || uncompressed_size == 0 ||
      uncompressed_size > std::numeric_limits<int>::max()) {
    return DecodeResult::kError;
  }
  parse_result =
      ParseVarint32FromString(pending_input_, &offset, &compressed_size);
  if (parse_result == VarintParseResult::kIncomplete) {
    return DecodeResult::kIncomplete;
  }
  if (parse_result == VarintParseResult::kError ||
      compressed_size > std::numeric_limits<int>::max()) {
    return DecodeResult::kError;
  }
  const uint32_t stored_size =
      compressed_size == 0 ? uncompressed_size : compressed_size;
  if (pending_input_.size() - offset < stored_size) {
    return DecodeResult::kIncomplete;
  }
#ifndef LZ4
  return DecodeResult::kError;
#else
  if (compressed_size == 0) {
    pending_output_.assign(pending_input_.data() + offset, uncompressed_size);
    pending_input_.erase(0, offset + uncompressed_size);
    output_offset_ = 0;
    return DecodeResult::kDone;
  }
  pending_output_.assign(uncompressed_size, '\0');
  int decompress_bytes = LZ4_decompress_safe(
      pending_input_.data() + offset, &pending_output_[0],
      static_cast<int>(compressed_size), static_cast<int>(uncompressed_size));
  if (decompress_bytes != static_cast<int>(uncompressed_size)) {
    return DecodeResult::kError;
  }
  pending_input_.erase(0, offset + compressed_size);
  output_offset_ = 0;
  return DecodeResult::kDone;
#endif
}

int LZ4StreamingUncompress::Uncompress(const char* input, size_t input_size,
                                       char* output, size_t* output_pos) {
  assert(output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  if (input != nullptr && input_size > 0) {
    pending_input_.append(input, input_size);
  }
  size_t copied = 0;
  while (copied < max_output_len_) {
    if (output_offset_ == pending_output_.size()) {
      pending_output_.clear();
      output_offset_ = 0;
      if (pending_input_.empty()) {
        break;
      }
      DecodeResult decode_result = DecodeNextChunk();
      if (decode_result == DecodeResult::kIncomplete) {
        break;
      }
      if (decode_result == DecodeResult::kError) {
        Reset();
        return -1;
      }
    }
    const size_t available = pending_output_.size() - output_offset_;
    const size_t copy_size = std::min(max_output_len_ - copied, available);
    if (copy_size == 0) {
      break;
    }
    std::memcpy(output + copied, pending_output_.data() + output_offset_,
                copy_size);
    output_offset_ += copy_size;
    copied += copy_size;
  }
  *output_pos = copied;
  return 0;
}

void LZ4StreamingUncompress::Reset() {
  pending_input_.clear();
  pending_output_.clear();
  output_offset_ = 0;
}

int ZSTDStreamingCompress::Compress(const char* input, size_t input_size,
                                    char* output, size_t* output_pos) {
  assert(input != nullptr && output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to compress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifndef ZSTD_ADVANCED
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#else
  if (input_buffer_.src == nullptr || input_buffer_.src != input) {
    // New input
    // Catch errors where the previous input was not fully decompressed.
    assert(input_buffer_.pos == input_buffer_.size);
    input_buffer_ = {input, input_size, /*pos=*/0};
  } else if (input_buffer_.src == input) {
    // Same input, not fully compressed.
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  const size_t remaining =
      ZSTD_compressStream2(cctx_, &output_buffer, &input_buffer_, ZSTD_e_end);
  if (ZSTD_isError(remaining)) {
    // Failure
    Reset();
    return -1;
  }
  // Success
  *output_pos = output_buffer.pos;
  return (int)remaining;
#endif
}

void ZSTDStreamingCompress::Reset() {
#ifdef ZSTD_ADVANCED
  ZSTD_CCtx_reset(cctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

int ZSTDStreamingUncompress::Uncompress(const char* input, size_t input_size,
                                        char* output, size_t* output_pos) {
  assert(output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to uncompress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifdef ZSTD_ADVANCED
  if (input) {
    // New input
    input_buffer_ = {input, input_size, /*pos=*/0};
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  size_t ret = ZSTD_decompressStream(dctx_, &output_buffer, &input_buffer_);
  if (ZSTD_isError(ret)) {
    Reset();
    return -1;
  }
  *output_pos = output_buffer.pos;
  return (int)(input_buffer_.size - input_buffer_.pos);
#else
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#endif
}

void ZSTDStreamingUncompress::Reset() {
#ifdef ZSTD_ADVANCED
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

}  // namespace ROCKSDB_NAMESPACE
