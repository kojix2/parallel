require "./core"

# Optimized extension for Indexable types (Array, Slice, etc.)
module Indexable(T)
  # Optimized parallel map for indexable collections
  # Uses unsafe_fetch for better performance and guarantees order preservation
  # Uses robust error handling internally but maintains fail-fast behavior
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 }           # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    return [] of U if size == 0

    results = Channel(Tuple(Int32, U) | Exception).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(size)

    if chunk_size > 1 && size > chunk_size
      # Chunk processing mode
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
              Parallel.log_fiber_exception(ex, "chunk #{chunk_idx}, index #{index}")
            end
          end
        end
      end
    else
      # Element-wise processing mode
      (0...size).each do |index|
        context.spawn do
          begin
            result = block.call(unsafe_fetch(index))
            results.send({index, result})
          rescue ex
            results.send(ex)
            Parallel.log_fiber_exception(ex, "index #{index}")
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

    # Sort by index and extract values
    temp_results.sort_by(&.[0]).map(&.[1])
  end

  # Optimized parallel each for indexable collections
  # Uses robust error handling internally but maintains fail-fast behavior
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    return if size == 0

    errors = Channel(Exception).new
    completed = Channel(Nil).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(size)

    if chunk_size > 1 && size > chunk_size
      # Chunk processing mode
      chunks = (0...size).each_slice(chunk_size).to_a

      chunks.each_with_index do |chunk_indices, chunk_idx|
        context.spawn do
          chunk_indices.each do |index|
            begin
              block.call(unsafe_fetch(index))
            rescue ex
              errors.send(ex)
              Parallel.log_fiber_exception(ex, "chunk #{chunk_idx}, index #{index}")
            end
          end
          completed.send(nil)
        end
      end

      collected_errors = Parallel.handle_each_errors_safe(errors, completed, chunks.size)
    else
      # Element-wise processing mode
      (0...size).each do |index|
        context.spawn do
          begin
            block.call(unsafe_fetch(index))
            completed.send(nil)
          rescue ex
            errors.send(ex)
            Parallel.log_fiber_exception(ex, "index #{index}")
          end
        end
      end

      collected_errors = Parallel.handle_each_errors_safe(errors, completed, size)
    end

    # Raise the first error if any occurred (fail-fast behavior)
    unless collected_errors.empty?
      raise collected_errors.first
    end
  end
end
