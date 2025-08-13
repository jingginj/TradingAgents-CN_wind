import os

# 确保在导入任何数据源管理器前设置默认数据源为 wind
os.environ['DEFAULT_CHINA_DATA_SOURCE'] = 'wind'
os.environ['TRADINGAGENTS_LOG_LEVEL'] = os.environ.get('TRADINGAGENTS_LOG_LEVEL', 'DEBUG')

from tradingagents.dataflows.interface import (
    get_current_china_data_source,
    get_china_stock_data_unified,
)


def main():
    print("ENV DEFAULT_CHINA_DATA_SOURCE=", os.getenv('DEFAULT_CHINA_DATA_SOURCE'))
    print("CURRENT_SOURCE_INFO:")
    try:
        print(get_current_china_data_source())
    except Exception as e:
        print("get_current_china_data_source error:", e)

    symbol = '000001.SZ'
    print(f"CALL get_china_stock_data_unified({symbol})...\n")
    try:
        res = get_china_stock_data_unified(symbol, '2024-01-01', '2024-03-01')
        head = (res or '')[:800]
        print("RESULT_HEAD:\n" + head)
        print("\nCHECK_DATA_SOURCE_WIND:", ("Wind" in head) or ("wind" in head))
    except Exception as e:
        print("get_china_stock_data_unified error:", e)


if __name__ == "__main__":
    main() 