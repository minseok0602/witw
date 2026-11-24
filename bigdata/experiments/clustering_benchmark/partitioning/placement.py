"""Whole-theme deterministic LPT placement. No Driver/Executor routing policy."""
import heapq

CAP = 2000


def predicted_cost(n, model):
    x = min(CAP, n) / 1000.0
    a, b, c = model['coefficients_quadratic_linear_constant']
    return max(1e-6, a*x*x + b*x + c)


def placement(counts, partitions, strategy, model=None):
    if partitions < 1 or not counts or any(n < 1 for n in counts.values()):
        raise ValueError('Positive partitions and nonempty positive theme counts required')
    if strategy == 'to_be_1':
        return {theme: int(theme) % partitions for theme in counts}
    if strategy == 'to_be_2':
        weights = {theme: min(CAP, n) for theme, n in counts.items()}
    elif strategy == 'to_be_3':
        if model is None:
            raise ValueError('Cost model must be frozen before test execution')
        weights = {theme: predicted_cost(n, model) for theme, n in counts.items()}
    else:
        raise ValueError(strategy)
    loads = [(0., pid) for pid in range(partitions)]
    heapq.heapify(loads)
    result = {}
    for theme in sorted(counts, key=lambda t: (-weights[t], t)):
        load, pid = heapq.heappop(loads)
        result[theme] = pid
        heapq.heappush(loads, (load + weights[theme], pid))
    return result
