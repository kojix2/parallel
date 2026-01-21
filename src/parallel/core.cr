require "fiber/execution_context"

# Core module for parallel processing functionality
module Parallel
  # Determines optimal chunk size for adaptive chunking
  def self.adaptive_chunk_size(collection_size : Int32) : Int32
    return 1 if collection_size == 0

    cpu_count = Fiber::ExecutionContext.default_workers_count
    # Use adaptive chunking: aim for cpu_count * 2 chunks, but with reasonable limits
    auto_chunk_size = [1, (collection_size.to_f / (cpu_count * 2)).ceil.to_i].max
    # Clamp to reasonable bounds: min 1, max 1000 to avoid too fine or too coarse granularity
    auto_chunk_size.clamp(1, 1000)
  end

  # Logs fiber exceptions with context information
  def self.log_fiber_exception(ex : Exception, task_info : String? = nil)
    if task_info
      Crystal.print_buffered("Unhandled exception in parallel task (%s): %s", task_info, ex.message, exception: ex, to: STDERR)
    else
      Crystal.print_buffered("Unhandled exception in parallel task: %s", ex.message, exception: ex, to: STDERR)
    end
  end

  # Unified empty check for collections
  # Returns {is_empty, estimated_size}
  def self.check_empty_and_size(collection) : Tuple(Bool, Int32)
    if collection.responds_to?(:size)
      size = collection.size
      {size == 0, size}
    else
      # Fallback: check if empty without materializing the entire collection
      empty = true
      collection.each do |_|
        empty = false
        break
      end
      {empty, 100} # fallback size for unknown collections
    end
  end

  # Validates and normalizes chunk size parameter
  # Returns the validated chunk size or raises an exception for invalid values
  def self.validate_chunk_size(chunk : Int32?, collection_size : Int32) : Int32
    if chunk.nil?
      return adaptive_chunk_size(collection_size)
    end

    if chunk <= 0
      raise ArgumentError.new("Chunk size must be a positive integer, got: #{chunk}")
    end

    # Warn if chunk size is larger than collection size (still valid but potentially inefficient)
    if chunk > collection_size && collection_size > 0
      # Allow but use collection_size as effective chunk size to avoid empty chunks
      collection_size
    else
      chunk
    end
  end

  # Common parallel map implementation for Indexable collections
  # This method handles the core logic for Indexable versions using direct index access
  def self.parallel_map_indexable(collection_size : Int32, context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : Int32 -> U) forall U
    return [] of U if collection_size == 0

    results = Channel(Tuple(Int32, U) | Exception).new

    if chunk_size > 1 && collection_size > chunk_size
      # Chunk processing mode
      chunks = (0...collection_size).each_slice(chunk_size).with_index.to_a

      chunks.each do |chunk_indices, chunk_idx|
        context.spawn do
          chunk_indices.each do |index|
            begin
              result = block.call(index)
              results.send({index, result})
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
    first_error = nil
    collection_size.times do
      case result = results.receive
      when Tuple(Int32, U)
        index, value = result
        result_array[index] = value
      when Exception
        first_error ||= result
      end
    end

    if first_error
      raise first_error
    end

    # Convert pointer to array
    Array(U).new(collection_size) { |i| result_array[i] }
  end

  # Common parallel map implementation for Enumerable collections using lazy evaluation
  # This method handles the core logic for Enumerable versions without creating intermediate arrays
  # Uses lock-free work-stealing approach for better performance
  def self.parallel_map_enumerable(enumerable : Enumerable(T), context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : T -> U) forall T, U
    # Try optimized approaches first, fallback to safe implementation for edge cases
    if enumerable.responds_to?(:size) && enumerable.size > 0
      parallel_map_enumerable_workstealing(enumerable, context, chunk_size, &block)
    else
      parallel_map_enumerable_safe(enumerable, context, chunk_size, &block)
    end
  end

  # Work-stealing optimized implementation for known-size enumerable (map version)
  private def self.parallel_map_enumerable_workstealing(enumerable : Enumerable(T), context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : T -> U) forall T, U
    # Convert to array with indices first to ensure consistent iteration
    items_with_indices = enumerable.each_with_index.to_a
    return [] of U if items_with_indices.empty?

    # Pre-chunk the items with indices to avoid iterator synchronization
    tasks_with_indices = items_with_indices.each_slice(chunk_size).to_a
    worker_count = Fiber::ExecutionContext.default_workers_count

    # Use atomic counter for lock-free task distribution
    task_index = Atomic(Int32).new(0)
    results = Channel(Tuple(Int32, U) | Exception).new
    completed = Channel(Nil).new

    # Spawn worker fibers
    worker_count.times do |worker_id|
      context.spawn do
        begin
          loop do
            # Atomically get and increment task index
            current_index = task_index.get(:acquire)
            # Use compare_and_set for thread-safe increment
            loop do
              break if current_index >= tasks_with_indices.size
              if task_index.compare_and_set(current_index, current_index + 1, :acquire, :acquire)
                break
              end
              current_index = task_index.get(:acquire)
            end

            break if current_index >= tasks_with_indices.size

            # Process the task chunk
            tasks_with_indices[current_index].each do |item, original_index|
              result = block.call(item)
              results.send({original_index, result})
            end
          end
        rescue ex
          results.send(ex)
          log_fiber_exception(ex, "worker #{worker_id}")
        ensure
          completed.send(nil)
        end
      end
    end

    # Collect results
    result_map = Hash(Int32, U).new
    collected_errors = [] of Exception
    completed_workers = 0

    loop do
      select
      when result = results.receive
        case result
        when Tuple(Int32, U)
          index, value = result
          result_map[index] = value
        when Exception
          collected_errors << result
        end
      when completed.receive
        completed_workers += 1
        break if completed_workers == worker_count
      end
    end

    # Raise the first error if any occurred
    unless collected_errors.empty?
      raise collected_errors.first
    end

    # Convert to sorted array by index
    result_map.to_a.sort_by(&.[0]).map(&.[1])
  end

  # Safe fallback implementation for unknown-size enumerable (map version)
  private def self.parallel_map_enumerable_safe(enumerable : Enumerable(T), context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : T -> U) forall T, U
    results = Channel(Tuple(Int32, U) | Exception).new
    completed = Channel(Nil).new

    # Use a mutex to synchronize iterator access (fallback for edge cases)
    mutex = Mutex.new
    iterator = enumerable.each_with_index
    finished = false

    # Spawn worker fibers
    worker_count = Fiber::ExecutionContext.default_workers_count
    worker_count.times do |worker_id|
      context.spawn do
        loop do
          items_with_index = [] of Tuple(T, Int32)

          # Get a chunk of items safely
          mutex.synchronize do
            break if finished

            chunk_size.times do
              begin
                item_with_index = iterator.next
                if item_with_index.is_a?(Iterator::Stop)
                  finished = true
                  break
                end
                items_with_index << item_with_index
              rescue ex
                finished = true
                results.send(ex)
                break
              end
            end
          end

          break if items_with_index.empty?

          # Process the chunk
          items_with_index.each do |item, index|
            begin
              result = block.call(item)
              results.send({index, result})
            rescue ex
              results.send(ex)
              log_fiber_exception(ex, "worker #{worker_id}, item #{index}")
              break
            end
          end
        end

        completed.send(nil)
      end
    end

    # Collect results
    result_map = Hash(Int32, U).new
    collected_errors = [] of Exception
    completed_workers = 0

    loop do
      select
      when result = results.receive
        case result
        when Tuple(Int32, U)
          index, value = result
          result_map[index] = value
        when Exception
          collected_errors << result
        end
      when completed.receive
        completed_workers += 1
        break if completed_workers == worker_count
      end
    end

    # Raise the first error if any occurred
    unless collected_errors.empty?
      raise collected_errors.first
    end

    # Convert to sorted array by index
    result_map.to_a.sort_by(&.[0]).map(&.[1])
  end

  # Lazy parallel each implementation for Enumerable collections
  # This method processes elements without materializing the entire collection
  # Uses lock-free work-stealing approach for better performance
  def self.parallel_each_enumerable(enumerable : Enumerable(T), context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : T -> _) forall T
    # Try optimized approaches first, fallback to safe implementation for edge cases
    if enumerable.responds_to?(:size) && enumerable.size > 0
      parallel_each_enumerable_workstealing(enumerable, context, chunk_size, &block)
    else
      parallel_each_enumerable_safe(enumerable, context, chunk_size, &block)
    end
  end

  # Work-stealing optimized implementation for known-size enumerable
  private def self.parallel_each_enumerable_workstealing(enumerable : Enumerable(T), context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : T -> _) forall T
    # Convert to array first to ensure consistent iteration
    items = enumerable.to_a
    return if items.empty?

    # Pre-chunk the items to avoid iterator synchronization
    tasks = items.each_slice(chunk_size).to_a
    worker_count = Fiber::ExecutionContext.default_workers_count

    # Use atomic counter for lock-free task distribution
    task_index = Atomic(Int32).new(0)
    worker_results = Channel(Exception?).new

    # Spawn worker fibers
    worker_count.times do |worker_id|
      context.spawn do
        worker_error = nil
        begin
          loop do
            # Atomically get and increment task index
            current_index = task_index.get(:acquire)
            # Use compare_and_set for thread-safe increment
            loop do
              break if current_index >= tasks.size
              if task_index.compare_and_set(current_index, current_index + 1, :acquire, :acquire)
                break
              end
              current_index = task_index.get(:acquire)
            end

            break if current_index >= tasks.size

            # Process the task chunk
            tasks[current_index].each do |item|
              block.call(item)
            end
          end
        rescue ex
          worker_error ||= ex
          log_fiber_exception(ex, "worker #{worker_id}")
        ensure
          worker_results.send(worker_error)
        end
      end
    end

    # Wait for all workers to complete
    collected_errors = [] of Exception
    worker_count.times do
      if error = worker_results.receive
        collected_errors << error
      end
    end

    # Raise the first error if any occurred
    unless collected_errors.empty?
      raise collected_errors.first
    end
  end

  # Safe fallback implementation for unknown-size enumerable
  private def self.parallel_each_enumerable_safe(enumerable : Enumerable(T), context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : T -> _) forall T
    worker_results = Channel(Exception?).new

    # Use a mutex to synchronize iterator access (fallback for edge cases)
    mutex = Mutex.new
    iterator = enumerable.each
    finished = false

    # Spawn worker fibers
    worker_count = Fiber::ExecutionContext.default_workers_count
    worker_count.times do
      context.spawn do
        worker_error = nil
        loop do
          items = [] of T

          # Get a chunk of items safely
          mutex.synchronize do
            break if finished

            chunk_size.times do
              begin
                item = iterator.next
                if item.is_a?(Iterator::Stop)
                  finished = true
                  break
                end
                items << item
              rescue ex
                finished = true
                worker_error ||= ex
                break
              end
            end
          end

          break if items.empty?

          # Process the chunk
          items.each do |item|
            begin
              block.call(item)
            rescue ex
              worker_error ||= ex
              break
            end
          end
        end

        worker_results.send(worker_error)
      end
    end

    # Wait for all workers to complete
    collected_errors = [] of Exception
    worker_count.times do
      if error = worker_results.receive
        collected_errors << error
      end
    end

    # Raise the first error if any occurred
    unless collected_errors.empty?
      raise collected_errors.first
    end
  end

  # Common parallel each implementation
  # This method handles the core logic for both Enumerable and Indexable versions
  def self.parallel_each(collection_size : Int32, context : Fiber::ExecutionContext::Parallel, chunk_size : Int32, &block : Int32 -> _)
    return if collection_size == 0

    worker_results = Channel(Exception?).new

    if chunk_size > 1 && collection_size > chunk_size
      # Chunk processing mode
      chunks = (0...collection_size).each_slice(chunk_size).to_a

      chunks.each_with_index do |chunk_indices, chunk_idx|
        context.spawn do
          chunk_error = nil
          chunk_indices.each do |index|
            begin
              block.call(index)
            rescue ex
              chunk_error ||= ex
              log_fiber_exception(ex, "chunk #{chunk_idx}, index #{index}")
              break
            end
          end
          worker_results.send(chunk_error)
        end
      end

      collected_errors = [] of Exception
      chunks.size.times do
        if error = worker_results.receive
          collected_errors << error
        end
      end
    else
      # Element-wise processing mode
      (0...collection_size).each do |index|
        context.spawn do
          element_error = nil
          begin
            block.call(index)
          rescue ex
            element_error ||= ex
            log_fiber_exception(ex, "index #{index}")
          ensure
            worker_results.send(element_error)
          end
        end
      end

      collected_errors = [] of Exception
      collection_size.times do
        if error = worker_results.receive
          collected_errors << error
        end
      end
    end

    # Raise the first error if any occurred
    unless collected_errors.empty?
      raise collected_errors.first
    end
  end
end
