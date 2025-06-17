require "./core"

# Extension for Enumerable types (Array, Hash, Set, Range, etc.)
module Enumerable(T)
  # Parallel map operation
  # Applies the given block to each element in parallel and returns an array of results
  # Uses robust error handling internally but maintains fail-fast behavior
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 }           # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    return items.map(&block) if items.empty?

    results = Channel(Tuple(Int32, U) | Exception).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(items.size)

    if chunk_size > 1 && items.size > chunk_size
      # Chunk processing mode
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
              Parallel.log_fiber_exception(ex, "chunk #{chunk_idx}, item #{item_idx}")
            end
          end
        end
      end
    else
      # Element-wise processing mode
      items.each_with_index do |item, idx|
        context.spawn do
          begin
            result = block.call(item)
            results.send({idx, result})
          rescue ex
            results.send(ex)
            Parallel.log_fiber_exception(ex, "item #{idx}")
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
  # Uses robust error handling internally but maintains fail-fast behavior
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    return if items.empty?

    errors = Channel(Exception).new
    completed = Channel(Nil).new
    chunk_size = chunk || Parallel.adaptive_chunk_size(items.size)

    if chunk_size > 1 && items.size > chunk_size
      # Chunk processing mode
      chunks = items.each_slice(chunk_size).to_a

      chunks.each_with_index do |chunk_items, chunk_idx|
        context.spawn do
          chunk_items.each_with_index do |item, item_idx|
            begin
              block.call(item)
            rescue ex
              errors.send(ex)
              Parallel.log_fiber_exception(ex, "chunk #{chunk_idx}, item #{item_idx}")
            end
          end
          completed.send(nil)
        end
      end

      collected_errors = Parallel.handle_each_errors_safe(errors, completed, chunks.size)
    else
      # Element-wise processing mode
      items.each_with_index do |item, idx|
        context.spawn do
          begin
            block.call(item)
            completed.send(nil)
          rescue ex
            errors.send(ex)
            Parallel.log_fiber_exception(ex, "item #{idx}")
          end
        end
      end

      collected_errors = Parallel.handle_each_errors_safe(errors, completed, items.size)
    end

    # Raise the first error if any occurred (fail-fast behavior)
    unless collected_errors.empty?
      raise collected_errors.first
    end
  end
end
