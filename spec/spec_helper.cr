require "spec"
require "../src/parallel"

def with_dedicated_execution_context(workers : Int32 = 2, &)
  previous = Parallel.execution_context
  Parallel.execution_context = Fiber::ExecutionContext::Parallel.new("spec-workers", workers)
  yield
ensure
  Parallel.execution_context = previous.not_nil!
end

def assert_completes_within(duration : Time::Span, &block)
  done = Channel(Exception?).new

  spawn do
    begin
      block.call
      done.send(nil)
    rescue ex
      done.send(ex)
    end
  end

  select
  when result = done.receive
    raise result if result
  when timeout duration
    raise "timeout waiting for block to complete within #{duration}"
  end
end
