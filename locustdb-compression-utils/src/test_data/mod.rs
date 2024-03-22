mod floats;

use floats::*;


pub static FLOATS: &[(&[f64], &str)] = &[
    (
        TRACE_ROLLOUT_FORWARD_ACTION_HEADS,
        "trace/rollout/forward/action_heads",
    ),
    (ACTIONS_ACT_RIGHT_UP, "actions/act_right_up"),
    (EPISODE_LENGTH_MEAN, "episode_length.mean"),
    (REWARD_MAX, "reward.max"),
    (GRADNORM, "gradnorm"),
    (SPS, "sps"),
    (APPROX_KL, "approxkl"),
    (POLICY_LOSS, "policy_loss"),
    (TRACE_BROADCAST_ADVANTAGES, "trace/update.optimize.broadcast_advantages"),
    (STEP, "step"),
];