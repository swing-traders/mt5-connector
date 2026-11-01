"""The ticks MT5's "Every tick" mode generates from an M1 bar, by the fitted rules, written from
their formulas alone: prices in integer points, the direction resolved over the bars before.

A bar is `(o, h, low, c, v)` in integer points with its tick volume; `d` is +1 for rising, -1 for
falling."""

import math

# The documented split (opening, range, closing) of the nominal reference count beyond Open.
SPLIT = {
    3: (1, 1, 1),
    4: (1, 2, 1),
    5: (1, 3, 1),
    6: (1, 4, 1),
    7: (2, 3, 2),
    8: (2, 4, 2),
    9: (2, 5, 2),
    10: (2, 6, 2),
    11: (3, 5, 3),
}


def sign(value: int) -> int:
    return (value > 0) - (value < 0)


def directions(bars, last: int | None = None) -> list[int]:
    """Each bar's direction: rising or falling by its close against its open, a doji the opposite
    of the last bar before it whose close differs from its open, and rising when there is none;
    `last` is the direction of the last such bar before the first, None when history holds none."""
    resolved = []
    for o, _h, _l, c, _v in bars:
        if c != o:
            last = sign(c - o)
            resolved.append(last)
        elif last is None:
            resolved.append(1)
        else:
            resolved.append(-last)
    return resolved


def extremes(h: int, low: int, d: int) -> tuple[int, int]:
    """The opening-side extreme E and the terminal extreme T."""
    if d > 0:
        return low, h
    else:
        return h, low


def split(o: int, h: int, low: int, c: int, v: int, d: int) -> tuple[int, int, int]:
    """The reference counts (a, r, z) of the opening shadow, the range and the closing shadow."""
    e, t = extremes(h, low, d)
    a, r, z = SPLIT[min(11, v - 1)]
    if o == e:
        r += a
        a = 0
    if c == t:
        r += z
        z = 0
    if r % 2 == 0:
        r -= 1
        if a == 2:
            a = 3
        elif z == 2:
            z = 3
    return a, r, z


def opening(o: int, e: int, a: int, d: int) -> list[int]:
    if a == 3:
        first, second = (o + 3 * e) // 4, (o + e) // 2
        if first == second:
            return [o - d, o, e]
        else:
            return [first, second, e]
    elif a == 2:
        return [o + d, e]
    elif a == 1:
        return [e]
    else:
        return []


def closing(t: int, c: int, z: int, d: int, h: int, low: int) -> list[int]:
    if z == 3:
        first, second = (t + 3 * c) // 4, (t + c) // 2
        if first == second:
            return [t - d, t, c]
        else:
            return [first, second, c]
    elif z == 2:
        if low <= c - d <= h:
            return [c - d, c]
        else:
            return [t - d, c]
    elif z == 1:
        return [c]
    else:
        return []


def waves(e: int, t: int, r: int, d: int, h: int, low: int) -> list[int]:
    """The r range points from E to T."""
    w = (r + 1) // 2
    step = (h - low - 1) // w + 1
    points = []
    previous = e
    for _ in range((r - 1) // 2):
        forward = previous + d * step
        rollback = forward - d
        points += [forward, rollback]
        previous = rollback
    return points + [t]


def reversed_outside(proposals: list[int], anchor: int, h: int, low: int) -> list[int]:
    """Each proposed price, a step from the preceding emitted one, emitted as the step the other way
    when it would leave [L, H]."""
    emitted = []
    for proposal in proposals:
        step = proposal - anchor
        if low <= proposal <= h:
            price = proposal
        else:
            price = anchor - step
        assert low <= price <= h, ("both steps leave the bar", anchor, proposal, low, h)
        emitted.append(price)
        anchor = price
    return emitted


def scaffold(o: int, h: int, low: int, c: int, v: int, d: int) -> list[int]:
    """Open followed by the opening, range and closing reference points."""
    e, t = extremes(h, low, d)
    a, r, z = split(o, h, low, c, v, d)
    proposals = opening(o, e, a, d) + waves(e, t, r, d, h, low) + closing(t, c, z, d, h, low)
    return [o] + reversed_outside(proposals, o, h, low)


def segment(a: int, b: int, n: int, h: int, low: int) -> list[int]:
    """The prices from A, excluded, to B, included, of a segment given n ticks."""
    distance = abs(b - a)
    s = sign(b - a)
    if n <= distance:
        proposals = [a + math.trunc(k * (b - a) / n) for k in range(1, n)] + [b]
    else:
        u = (b - a) / n
        bounded = n % 2 == 0 and not low <= a - s <= h
        m = n - 1 if bounded else n
        toward = n % 2 == 1 or bounded
        x = float(a)
        added = 0
        proposals = []
        previous = a
        for k in range(1, m):
            if toward:
                j = (k + 1) // 2
            else:
                j = k // 2
            while added < j:
                x += u
                added += 1
            base = a + math.trunc(x - a)
            if k % 2 == 1 and toward:
                candidate = base + s
            elif k % 2 == 1:
                candidate = base - s
            else:
                candidate = base
            if candidate != previous:
                proposals.append(candidate)
                previous = candidate
        if b != previous:
            proposals.append(b)
    return reversed_outside(proposals, a, h, low)


def allocation(v: int, points: list[int], h: int, low: int) -> list[tuple[int, list[int]]]:
    """Each segment's nominal count and emitted prices, the scaffold `points` including Open and
    Close."""
    segments = len(points) - 1
    quota = max(1, (v - 2) // segments)
    emitted = 0
    allocated = []
    for i in range(segments):
        left = segments - i
        budget = v - 1 - emitted
        if left == 1:
            n = budget
        elif quota == 1 and budget > left + 1:
            n = 2
        elif quota == 1:
            n = 1
        elif quota == 2:
            n = max(1, min(max(2, (budget - 1) // left), budget - left))
        else:
            n = quota
        prices = segment(points[i], points[i + 1], n, h, low)
        allocated.append((n, prices))
        emitted += len(prices)
    return allocated


def bids(o: int, h: int, low: int, c: int, v: int, d: int) -> list[int]:
    """The minute's bids in integer points; a bar recording no tick volume has none."""
    if v == 0:
        return []
    elif v >= 2 and o == h == low == c:
        return [o] * v
    elif v == 1:
        return [c]
    elif v == 2:
        return [o, c]
    elif v == 3 and ((o == low and c == h) or (o == h and c == low)):
        return [o, (o + c) // 2, c]
    elif v == 3 and (o == low or c == low):
        return [o, h, c]
    elif v == 3:
        return [o, low, c]
    elif v <= 5:
        return low_volume(o, h, low, c, v, d)
    else:
        points = scaffold(o, h, low, c, v, d)
        prices = [o]
        for _n, emitted in allocation(v, points, h, low):
            prices += emitted
        return prices


def low_volume(o: int, h: int, low: int, c: int, v: int, d: int) -> list[int]:
    """V of 4 or 5: the scaffold, one midpoint added where it is one short."""
    points = scaffold(o, h, low, c, v, d)
    assert len(points) in (v, v - 1), ("a low-volume scaffold more than one short", points, v)
    if len(points) == v:
        return points
    e, t = extremes(h, low, d)
    if v == 4 and o == e and c != t:
        j = 0
    else:
        j = max(range(len(points) - 1), key=lambda k: (abs(points[k + 1] - points[k]), k))
    a, b = points[j], points[j + 1]
    if o == e and c == t:
        middle = a + math.trunc((b - a) / 2)
    else:
        middle = (a + b) // 2
    return points[: j + 1] + [middle] + points[j + 1 :]


def offsets_ms(t: int, v: int, n: int) -> list[int]:
    """Each tick's offset from the bar's open in milliseconds, `t` the raw broker bar-open epoch in
    seconds and n the emitted count."""
    if v == 0:
        return []
    elif v == 1:
        return [30_000]
    elif v == 2:
        return [0, 30_000]
    elif v == 3:
        return [0, 30_000, 59_000]
    clock = float(t)
    delta = 60.0 / (v + 1)
    offsets = []
    for _ in range(n - 1):
        if delta >= 1:
            offsets.append(math.trunc(clock) * 1000 - t * 1000)
        else:
            offsets.append(math.trunc(clock * 1000) - t * 1000)
        clock += delta
    if v <= 59:
        offsets.append(59_000)
    else:
        offsets.append(59_000 + math.ceil((((n - 1) * 60_000) % ((v + 1) * 1000)) / (v + 1)))
    return offsets


def ask(bid: float, spread: int, point: float, digits: int) -> float:
    """The ask of a bid: the bid plus the bar's spread in points, at the symbol's digits."""
    return round(bid + spread * point, digits)
