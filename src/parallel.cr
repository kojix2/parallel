# Parallel processing library for Crystal using Fiber::ExecutionContext
#
# This library extends Enumerable and Indexable with parallel processing methods.
# It requires Crystal 1.6.0+ and the following flags:
# -Dpreview_mt -Dexecution_context
#
# Example:
# ```
# [1, 2, 3, 4].par_map { |x| x * 2 } # => [2, 4, 6, 8]
# ```
module Parallel
  VERSION = {{ `shards version #{__DIR__}`.chomp.stringify }}

  # Global ExecutionContext for parallel processing
  # Reusing a single context is recommended for performance
  PARALLEL_CONTEXT = Fiber::ExecutionContext::MultiThreaded.new(
    "parallel-workers",
    Fiber::ExecutionContext.default_workers_count
  )

  # Determines optimal chunk size for adaptive chunking
  def self.adaptive_chunk_size(collection_size : Int32) : Int32
    return 1 if collection_size == 0
    
    cpu_count = Fiber::ExecutionContext.default_workers_count
    # Use adaptive chunking: aim for cpu_count * 2 chunks, but with reasonable limits
    auto_chunk_size = [1, (collection_size.to_f / (cpu_count * 2)).ceil.to_i].max
    # Clamp to reasonable bounds: min 1, max 1000 to avoid too fine or too coarse granularity
    auto_chunk_size.clamp(1, 1000)
  end

  # Handles parallel each operation error collection
  def self.handle_each_errors(errors_channel, completed_channel, expected_count : Int32)
    expected_count.times do
      select
      when error = errors_channel.receive
        raise error
      when completed_channel.receive
        # Continue
      end
    end
  end
end

# Extension for Enumerable types (Array, Hash, Set, Range, etc.)
module Enumerable(T)
  # Parallel map operation
  # Applies the given block to each element in parallel and returns an array of results
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 } # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    return items.map(&block) if items.empty?  # Handle empty collections
    
    results = Channel(Tuple(Int32, U) | Exception).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(items.size)
    
    if chunk_size > 1 && items.size > chunk_size
      # Chunk processing mode - process multiple elements per fiber to reduce context switches
      chunks = items.each_slice(chunk_size).with_index.to_a

      chunks.each do |chunk_items, chunk_idx|
        context.spawn do
          chunk_items.each_with_index do |item, item_idx|
            begin
              result = block.call(item)
              global_idx = chunk_idx * chunk_size + item_idx
              results.send({global_idx, result})
            rescue ex
              results.send(ex)
            end
          end
        end
      end
    else
      # Element-wise processing mode (for small collections or chunk_size = 1)
      items.each_with_index do |item, idx|
        context.spawn do
          begin
            result = block.call(item)
            results.send({idx, result})
          rescue ex
            results.send(ex)
          end
        end
      end
    end

    temp_results = Array(Tuple(Int32, U)).new
    items.size.times do
      case result = results.receive
      when Tuple(Int32, U)
        temp_results << result
      when Exception
        raise result
      end
    end

    # Sort by index and extract values
    temp_results.sort_by(&.[0]).map(&.[1])
  end

  # Parallel each operation
  # Applies the given block to each element in parallel (no return value)
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    return if items.empty?  # Handle empty collections
    
    errors = Channel(Exception).new
    completed = Channel(Nil).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(items.size)
    
    if chunk_size > 1 && items.size > chunk_size
      # Chunk processing mode - process multiple elements per fiber to reduce context switches
      chunks = items.each_slice(chunk_size).to_a

      chunks.each do |chunk_items|
        context.spawn do
          chunk_items.each do |item|
            begin
              block.call(item)
            rescue ex
              errors.send(ex)
              next
            end
          end
          completed.send(nil)
        end
      end

      Parallel.handle_each_errors(errors, completed, chunks.size)
    else
      # Element-wise processing mode (for small collections or chunk_size = 1)
      items.each do |item|
        context.spawn do
          begin
            block.call(item)
            completed.send(nil)
          rescue ex
            errors.send(ex)
          end
        end
      end

      Parallel.handle_each_errors(errors, completed, items.size)
    end
  end
end

# Optimized extension for Indexable types (Array, Slice, etc.)
module Indexable(T)
  # Optimized parallel map for indexable collections
  # Uses unsafe_fetch for better performance and guarantees order preservation
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 } # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    return [] of U if size == 0  # Handle empty collections
    
    results = Channel(Tuple(Int32, U) | Exception).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(size)
    
    if chunk_size > 1 && size > chunk_size
      # Chunk processing mode - process multiple elements per fiber to reduce context switches
      chunks = (0...size).each_slice(chunk_size).with_index.to_a

      chunks.each do |chunk_indices, chunk_idx|
        context.spawn do
          chunk_indices.each_with_index do |index, item_idx|
            begin
              result = block.call(unsafe_fetch(index))
              global_idx = chunk_idx * chunk_size + item_idx
              results.send({global_idx, result})
            rescue ex
              results.send(ex)
            end
          end
        end
      end
    else
      # Element-wise processing mode (for small collections or chunk_size = 1)
      (0...size).each do |index|
        context.spawn do
          begin
            result = block.call(unsafe_fetch(index))
            results.send({index, result})
          rescue ex
            results.send(ex)
          end
        end
      end
    end

    temp_results = Array(Tuple(Int32, U)).new
    size.times do
      case result = results.receive
      when Tuple(Int32, U)
        temp_results << result
      when Exception
        raise result
      end
    end

    # Sort by index and extract values to maintain order
    temp_results.sort_by(&.[0]).map(&.[1])
  end

  # Optimized parallel each for indexable collections
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    return if size == 0  # Handle empty collections
    
    errors = Channel(Exception).new
    completed = Channel(Nil).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(size)
    
    if chunk_size > 1 && size > chunk_size
      # Chunk processing mode - process multiple elements per fiber to reduce context switches
      chunks = (0...size).each_slice(chunk_size).to_a

      chunks.each do |chunk_indices|
        context.spawn do
          chunk_indices.each do |index|
            begin
              block.call(unsafe_fetch(index))
            rescue ex
              errors.send(ex)
              next
            end
          end
          completed.send(nil)
        end
      end

      Parallel.handle_each_errors(errors, completed, chunks.size)
    else
      # Element-wise processing mode (for small collections or chunk_size = 1)
      (0...size).each do |index|
        context.spawn do
          begin
            block.call(unsafe_fetch(index))
            completed.send(nil)
          rescue ex
            errors.send(ex)
          end
        end
      end

      Parallel.handle_each_errors(errors, completed, size)
    end
  end
end
