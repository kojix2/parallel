module Parallel
  # Default ExecutionContext for parallel processing
  @@execution_context : Fiber::ExecutionContext::Parallel?

  # Returns the current default context. Lazily creates a dedicated context.
  def self.execution_context : Fiber::ExecutionContext::Parallel
    @@execution_context ||= Fiber::ExecutionContext::Parallel.new(
      "parallel-workers",
      Fiber::ExecutionContext.default_workers_count
    )
  end

  # Sets the default context explicitly.
  def self.execution_context=(context : Fiber::ExecutionContext::Parallel) : Fiber::ExecutionContext::Parallel
    @@execution_context = context
  end
end
