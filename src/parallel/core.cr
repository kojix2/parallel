# Core module for parallel processing functionality
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

  # Handles parallel each operation with error collection (new behavior - collect all errors)
  def self.handle_each_errors_safe(errors_channel, completed_channel, expected_count : Int32) : Array(Exception)
    collected_errors = [] of Exception
    expected_count.times do
      select
      when error = errors_channel.receive
        collected_errors << error
      when completed_channel.receive
        # Continue
      end
    end
    collected_errors
  end

  # Logs fiber exceptions with context information
  def self.log_fiber_exception(ex : Exception, task_info : String? = nil)
    if task_info
      Crystal.print_buffered("Unhandled exception in parallel task (%s): %s", task_info, ex.message, exception: ex, to: STDERR)
    else
      Crystal.print_buffered("Unhandled exception in parallel task: %s", ex.message, exception: ex, to: STDERR)
    end
  end

  # Ensures cleanup is performed even if exceptions occur
  def self.ensure_cleanup(&cleanup_block)
    at_exit do |exit_code, exception|
      cleanup_block.call
    end
  end

  # Common parallel map implementation
  # This method handles the core logic for both Enumerable and Indexable versions
  def self.parallel_map(collection_size : Int32, context : Fiber::ExecutionContext::MultiThreaded, chunk_size : Int32, &block : Int32 -> U) forall U
    return [] of U if collection_size == 0

    results = Channel(Tuple(Int32, U) | Exception).new

    if chunk_size > 1 && collection_size > chunk_size
      # Chunk processing mode
      chunks = (0...collection_size).each_slice(chunk_size).with_index.to_a

      chunks.each do |chunk_indices, chunk_idx|
        context.spawn do
          chunk_indices.each_with_index do |index, item_idx|
            begin
              result = block.call(index)
              global_idx = chunk_idx * chunk_size + item_idx
              results.send({global_idx, result})
            rescue ex
              results.send(ex)
              log_fiber_exception(ex, "chunk #{chunk_idx}, index #{index}")
            end
          end
        end
      end
    else
      # Element-wise processing mode
      (0...collection_size).each do |index|
        context.spawn do
          begin
            result = block.call(index)
            results.send({index, result})
          rescue ex
            results.send(ex)
            log_fiber_exception(ex, "index #{index}")
          end
        end
      end
    end

    # Pre-allocate result array and fill directly by index (no sorting needed)
    result_array = Pointer(U).malloc(collection_size)
    collection_size.times do
      case result = results.receive
      when Tuple(Int32, U)
        index, value = result
        result_array[index] = value
      when Exception
        raise result
      end
    end

    # Convert pointer to array
    Array(U).new(collection_size) { |i| result_array[i] }
  end

  # Common parallel each implementation
  # This method handles the core logic for both Enumerable and Indexable versions
  def self.parallel_each(collection_size : Int32, context : Fiber::ExecutionContext::MultiThreaded, chunk_size : Int32, &block : Int32 -> _)
    return if collection_size == 0

    errors = Channel(Exception).new
    completed = Channel(Nil).new

    if chunk_size > 1 && collection_size > chunk_size
      # Chunk processing mode
      chunks = (0...collection_size).each_slice(chunk_size).to_a

      chunks.each_with_index do |chunk_indices, chunk_idx|
        context.spawn do
          chunk_indices.each do |index|
            begin
              block.call(index)
            rescue ex
              errors.send(ex)
              log_fiber_exception(ex, "chunk #{chunk_idx}, index #{index}")
            end
          end
          completed.send(nil)
        end
      end

      collected_errors = handle_each_errors_safe(errors, completed, chunks.size)
    else
      # Element-wise processing mode
      (0...collection_size).each do |index|
        context.spawn do
          begin
            block.call(index)
            completed.send(nil)
          rescue ex
            errors.send(ex)
            log_fiber_exception(ex, "index #{index}")
          end
        end
      end

      collected_errors = handle_each_errors_safe(errors, completed, collection_size)
    end

    # Raise the first error if any occurred (fail-fast behavior)
    unless collected_errors.empty?
      raise collected_errors.first
    end
  end
end
