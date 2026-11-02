"""用户修正版动量的 C 预计算入口；列名与交易端 Python oracle 严格对应。"""
from factors.c_ext.momentum_v2_wrapper import inputs, calculate


def diagnostics(df, param):
    return calculate(df, param, inputs(df))


def signal_multi_params(df, param_list):
    prepared = inputs(df)
    return {str(p): calculate(df, p, prepared, diagnostic=False) for p in param_list}


def signal(*args):
    df, param, name = args[:3]
    df[name] = signal_multi_params(df, [param])[str(param)].to_numpy()
    return df
