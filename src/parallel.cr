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

require "./parallel/version"
require "./parallel/context"
require "./parallel/core"
require "./parallel/enumerable"
require "./parallel/indexable"
