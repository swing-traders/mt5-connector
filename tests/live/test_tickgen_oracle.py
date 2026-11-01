"""The fitted every-tick rules' oracle against each rule's worked example, every expectation derived
by hand from the rule's formula beside it. Prices are integer points."""

import tickgen_oracle as oracle


def test_a_bar_whose_last_saw_meets_its_low_emits_one_tick_fewer_than_its_volume():
    # d=-1: E=H=92415, T=L=92408; V-1=6 splits (1,4,1); even r=4 -> 3, so (a,r,z)=(1,3,1).
    # Opening [E]; range w=2, step=(7-1)//2+1=4: 92411, 92412, then T; closing [C]: the scaffold
    # (92414,92415,92411,92412,92408,92409). q=max(1,5//5)=1; budgets 6,5,4,3 never exceed
    # r_left+1, so n=1 each, until the last segment takes n=b=2: 92408->92409 is a saw, n even and
    # A-s=92407 below L, so m=1 and only B is emitted: six ticks.
    assert oracle.bids(92414, 92415, 92408, 92409, 7, -1) == [
        92414,
        92415,
        92411,
        92412,
        92408,
        92409,
    ]


def test_a_rising_doji_opening_at_its_low_moves_its_opening_points_to_the_range():
    # V-1=9 splits (2,5,2); O=E=L moves a=2 to r=7; C=92543 is not T=92550; r odd: (0,7,2).
    assert oracle.split(92543, 92550, 92543, 92543, 10, 1) == (0, 7, 2)


def test_a_doji_after_a_falling_bar_rises_and_visits_its_low_first():
    # The doji goes opposite the last non-doji, -1, so d=+1: E=L, T=H; V-1=3 splits (1,1,1),
    # nothing missing, r odd: Open, [E], [T], [C].
    falling, doji = (92510, 92512, 92498, 92500, 9), (92500, 92505, 92495, 92500, 4)

    assert oracle.directions([falling, doji]) == [-1, 1]
    assert oracle.bids(92500, 92505, 92495, 92500, 4, 1) == [92500, 92495, 92505, 92500]


def test_a_seven_volume_minute_emitting_six_ticks_is_stamped_on_the_eighth_of_a_minute_grid():
    # delta=60/8=7.5 s; the first N-1=5 ticks at t, t+7.5, t+15, t+22.5, t+30, truncated to whole
    # seconds since delta>=1: 0, 7, 15, 22, 30 s; V<=59 puts the last at 59 s.
    assert oracle.offsets_ms(1673225700, 7, 6) == [0, 7000, 15000, 22000, 30000, 59000]


def test_the_ask_is_the_bid_plus_the_bars_spread_in_points():
    # 0.92543 + 6 * 0.00001 = 0.92549.
    assert oracle.ask(0.92543, 6, 0.00001, 5) == 0.92549


def test_three_point_shadows_sit_at_the_floored_quarter_and_half_toward_their_extreme():
    # Opening toward E=H=92548: floor(370187/4)=92546, floor(185091/2)=92545, 92548.
    # Closing from T=L=92533 toward C=92538: floor(370147/4)=92536, floor(185071/2)=92535, 92538.
    assert oracle.opening(92543, 92548, 3, -1) == [92546, 92545, 92548]
    assert oracle.closing(92533, 92538, 3, -1, 92548, 92533) == [92536, 92535, 92538]


def test_five_range_points_step_toward_the_terminal_extreme_with_one_point_rollbacks():
    # w=3, step=(15-1)//3+1=5: 92548-5=92543, rollback 92544; 92539, rollback 92540; then T.
    assert oracle.waves(92548, 92533, 5, -1, 92548, 92533) == [92543, 92544, 92539, 92540, 92533]


def test_a_five_volume_bar_missing_both_shadows_gains_a_truncated_midpoint_in_its_last_segment():
    # d=-1: O=E=H, C=T=L. V-1=4 splits (1,2,1); both shadows move to r=4, even -> 3: (0,3,0).
    # Range w=2, step=(21-1)//2+1=11: 92365, 92366, then 92355; four points, one short of V.
    # Segments 11, 1, 11 tie, so the last: 92366+trunc(-11/2)=92361.
    assert oracle.bids(92376, 92376, 92355, 92355, 5, -1) == [92376, 92365, 92366, 92361, 92355]


def test_a_small_quota_bar_allocates_two_ticks_then_three_with_the_last_segment_taking_four():
    # (3,5,3), R=11; q=max(1,29//11)=2, so n=max(1,min(max(2,(b-1)//r_left),b-r_left)).
    # Scaffold 92368,92365,92366,92365,92376,92375,92386,92385,92397,92388,92391,92385.
    # b,r_left: 30,11 -> 2 (2 emitted); 28,10 -> 2, but 92365->92366 is a saw with A-s=92364
    # below L, so only B (1); 27,9 -> 2 (2); then 25,8; 22,7; 19,6; 16,5; 13,4; 10,3 -> 3 each, and
    # 7,2 -> min(3,5)=3; the last takes b=4.
    points = oracle.scaffold(92368, 92397, 92365, 92385, 31, 1)

    allocated = oracle.allocation(31, points, 92397, 92365)

    assert points == [
        92368,
        92365,
        92366,
        92365,
        92376,
        92375,
        92386,
        92385,
        92397,
        92388,
        92391,
        92385,
    ]
    assert [n for n, _ in allocated] == [2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4]
    assert [len(prices) for _, prices in allocated] == [2, 1, 2, 3, 3, 3, 3, 3, 3, 3, 4]


def test_a_segment_given_more_ticks_than_points_saws_away_first_and_drifts_toward_its_end():
    # D=4 < n=6: u=2/3, s=+1, n even and A-s=92565 inside, so m=6, away first.
    # k=1 j=0 base 92566 -> 92565; k=2 j=1 X=92566.67 -> 92566; k=3 j=1 -> 92565;
    # k=4 j=2 X=92567.33 -> 92567; k=5 j=2 -> 92566; then B.
    assert oracle.segment(92566, 92570, 6, 92570, 92543) == [
        92565,
        92566,
        92565,
        92567,
        92566,
        92570,
    ]


def test_a_step_that_would_leave_the_bar_is_taken_the_other_way():
    # 91839+1=91840 is above H=91839, so 91839-1.
    assert oracle.reversed_outside([91840], 91839, 91839, 91837) == [91838]


def test_a_rising_doji_at_its_low_closes_through_the_point_below_its_high():
    # (0,7,2): range w=4, step=(7-1)//4+1=2 from 92543: 92545,92544, 92546,92545, 92547,92546,
    # then T=92550. Closing C-d=92542 lies below L, so T-d=92549, then C. Ten points for V=10:
    # q=max(1,8//9)=1 and no budget exceeds r_left+1, so every segment emits its endpoint alone.
    assert oracle.bids(92543, 92550, 92543, 92543, 10, 1) == [
        92543,
        92545,
        92544,
        92546,
        92545,
        92547,
        92546,
        92550,
        92549,
        92543,
    ]


def test_a_doji_after_two_dojis_goes_opposite_the_last_non_doji_before_them():
    # Falling predecessor: the target rises, Open, L, H, C. Rising predecessor: it falls, E=H,
    # T=L: Open, H, L, C.
    doji = (92510, 92515, 92505, 92510, 6)
    target = (92500, 92505, 92495, 92500, 4)
    falling, rising = (92520, 92525, 92505, 92510, 8), (92500, 92515, 92495, 92510, 8)

    after_falling = oracle.directions([falling, doji, doji, target])[-1]
    after_rising = oracle.directions([rising, doji, doji, target])[-1]

    assert oracle.bids(*target, after_falling) == [92500, 92495, 92505, 92500]
    assert oracle.bids(*target, after_rising) == [92500, 92505, 92495, 92500]


def test_a_doji_at_the_start_of_history_rises_and_visits_its_low_first():
    # No bar before it differs from its open, so it rises (d=+1, E=L, T=H): Open, [E], [T], [C].
    doji = (92500, 92505, 92495, 92500, 4)

    direction = oracle.directions([doji])[0]

    assert direction == 1
    assert oracle.bids(*doji, direction) == [92500, 92495, 92505, 92500]


def test_absolute_coordinate_midpoints_and_shadows_floor_below_zero():
    # V=3 with O=L and C=H: the midpoint is floor((-10-1)/2)=floor(-5.5)=-6.
    # Opening toward E=-13 from O=-10: floor(-49/4)=-13 and floor(-23/2)=-12 differ, so both stand.
    assert oracle.bids(-10, -1, -10, -1, 3, 1) == [-10, -6, -1]
    assert oracle.opening(-10, -13, 3, 1) == [-13, -12, -13]


def test_a_bar_recording_no_tick_volume_emits_no_tick():
    # N=V=0: no bid and no stamp.
    assert oracle.bids(92500, 92505, 92495, 92502, 0, 1) == []
    assert oracle.offsets_ms(1673225700, 0, 0) == []
