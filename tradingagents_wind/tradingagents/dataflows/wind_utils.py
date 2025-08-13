#!/usr/bin/env python3
"""
Wind数据源工具类
提供A股市场数据获取功能，包括实时行情、历史数据、财务数据等
基于万得Wind API实现
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Union
import warnings
import time

# 导入日志模块
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('agents')
warnings.filterwarnings('ignore')

# 导入统一日志系统
# from tradingagents.utils.logging_init import get_logger

# 导入缓存管理器
try:
    from .cache_manager import get_cache
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    logger.warning("⚠️ 缓存管理器不可用")

# 导入Wind API
try:
    from WindPy import w
    WIND_AVAILABLE = True
except ImportError:
    WIND_AVAILABLE = False
    logger.error("❌ WindPy库未安装，请安装Wind客户端和WindPy")


class WindProvider:
    """Wind数据提供器"""
    
    def __init__(self, enable_cache: bool = True):
        """
        初始化Wind提供器
        
        Args:
            enable_cache: 是否启用缓存
        """
        self.connected = False
        self.enable_cache = enable_cache and CACHE_AVAILABLE
        
        # 初始化缓存管理器
        self.cache_manager = None
        if self.enable_cache:
            try:
                from .cache_manager import get_cache
                self.cache_manager = get_cache()
            except Exception as e:
                logger.warning(f"⚠️ 缓存管理器初始化失败: {e}")
                self.enable_cache = False

        # 延迟初始化Wind API连接，只在需要时连接
        self._connect_if_needed()
    
    def __del__(self):
        """析构函数：确保Wind连接被正确关闭"""
        self.disconnect()
    
    def disconnect(self):
        """断开Wind连接"""
        if self.connected and WIND_AVAILABLE:
            try:
                from WindPy import w
                w.stop()
                self.connected = False
                logger.info("🔄 Wind连接已断开")
            except Exception as e:
                logger.warning(f"⚠️ Wind连接断开失败: {e}")
    
    def _connect_if_needed(self):
        """按需建立Wind连接，包含重连逻辑"""
        if self.connected:
            return True
            
        if not WIND_AVAILABLE:
            logger.error("❌ Wind不可用，无法建立连接")
            return False
        
        try:
            from WindPy import w
            
            # 检查连接状态
            if hasattr(w, 'isconnected') and w.isconnected():
                self.connected = True
                logger.debug("✅ Wind连接已存在")
                return True
            
            # 建立新连接
            start_result = w.start()
            if start_result.ErrorCode == 0:
                self.connected = True
                logger.info("✅ Wind连接建立成功")
                return True
            else:
                logger.error(f"❌ Wind连接失败，错误代码: {start_result.ErrorCode}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Wind连接异常: {e}")
            return False
    
    def _ensure_connection(self):
        """确保Wind连接有效，包含重连逻辑"""
        if not self.connected:
            return self._connect_if_needed()
        
        # 检查现有连接是否仍然有效
        try:
            if WIND_AVAILABLE:
                from WindPy import w
                if hasattr(w, 'isconnected') and not w.isconnected():
                    logger.warning("⚠️ Wind连接已断开，尝试重连...")
                    self.connected = False
                    return self._connect_if_needed()
            return True
        except Exception as e:
            logger.warning(f"⚠️ Wind连接检查失败，尝试重连: {e}")
            self.connected = False
            return self._connect_if_needed()
    
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取A股股票列表
        
        Returns:
            DataFrame: 股票列表数据
        """
        # 确保Wind连接有效
        if not self._ensure_connection():
            logger.error(f"❌ Wind连接不可用，无法获取股票列表")
            return pd.DataFrame()
        
        try:
            # 尝试从缓存获取
            if self.enable_cache:
                cache_key = self.cache_manager.find_cached_stock_data(
                    symbol="wind_stock_list",
                    max_age_hours=24  # 股票列表缓存24小时
                )
                
                if cache_key:
                    cached_data = self.cache_manager.load_stock_data(cache_key)
                    if cached_data is not None:
                        # 检查是否为DataFrame且不为空
                        if hasattr(cached_data, 'empty') and not cached_data.empty:
                            logger.info(f"📦 从缓存获取股票列表: {len(cached_data)}条")
                            return cached_data
                        elif isinstance(cached_data, str) and cached_data.strip():
                            logger.info(f"📦 从缓存获取股票列表: 字符串格式")
                            return cached_data
            
            logger.info(f"🔄 从Wind获取A股股票列表...")
            
            # 获取上市证券概况
            wind_data = w.wset("listedsecuritygeneralview",
                "sectorid=a001010100000000;field=wind_code,sec_name,trade_status,ipo_date,province,sec_type,listing_board,exchange")
            
            if wind_data.ErrorCode != 0:
                logger.error(f"❌ Wind返回错误，错误代码: {wind_data.ErrorCode}")
                return pd.DataFrame()
            
            # 将Wind数据转换为DataFrame格式
            stock_list = pd.DataFrame(wind_data.Data).T  # 转置数据
            stock_list.columns = wind_data.Fields  # 设置列名
            
            if stock_list is not None and not stock_list.empty:
                # 标准化列名以保持与Tushare接口一致
                column_mapping = {
                    'WIND_CODE': 'ts_code',
                    'SEC_NAME': 'name', 
                    'TRADE_STATUS': 'list_status',
                    'IPO_DATE': 'list_date',
                    'PROVINCE': 'area',
                    'SEC_TYPE': 'market',
                    'LISTING_BOARD': 'industry',
                    'EXCHANGE': 'exchange'
                }
                
                # 重命名列
                for old_col, new_col in column_mapping.items():
                    if old_col in stock_list.columns:
                        stock_list[new_col] = stock_list[old_col]
                
                # 添加symbol列（去掉交易所后缀）
                if 'ts_code' in stock_list.columns:
                    stock_list['symbol'] = stock_list['ts_code'].str.split('.').str[0]
                
                logger.info(f"✅ 获取股票列表成功: {len(stock_list)}条")
                
                # 缓存数据
                if self.enable_cache and self.cache_manager:
                    try:
                        cache_key = self.cache_manager.save_stock_data(
                            symbol="wind_stock_list",
                            data=stock_list,
                            data_source="wind"
                        )
                        logger.info(f"💾 A股股票列表已缓存: wind_stock_list (wind) -> {cache_key}")
                    except Exception as e:
                        logger.error(f"⚠️ 缓存保存失败: {e}")
                
                return stock_list
            else:
                logger.warning(f"⚠️ Wind返回空数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_stock_daily(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取股票日线数据
        
        Args:
            symbol: 股票代码（如：000001.SZ）
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            
        Returns:
            DataFrame: 日线数据
        """
        # 记录详细的调用信息
        logger.info(f"🔍 [Wind详细日志] get_stock_daily 开始执行")
        logger.info(f"🔍 [Wind详细日志] 输入参数: symbol='{symbol}', start_date='{start_date}', end_date='{end_date}'")

        # 确保Wind连接有效
        if not self._ensure_connection():
            logger.error(f"❌ [Wind详细日志] Wind连接不可用，无法获取数据")
            return pd.DataFrame()

        logger.info(f"🔍 [Wind详细日志] 连接状态: {self.connected}")

        try:
            # 标准化股票代码
            logger.info(f"🔍 [股票代码追踪] get_stock_daily 调用 _normalize_symbol，传入参数: '{symbol}'")
            wind_code = self._normalize_symbol(symbol)
            logger.info(f"🔍 [股票代码追踪] _normalize_symbol 返回结果: '{wind_code}'")

            # 设置默认日期
            original_start = start_date
            original_end = end_date

            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
                logger.info(f"🔍 [Wind详细日志] 结束日期为空，设置为当前日期: {end_date}")
            else:
                # Wind API支持YYYY-MM-DD格式
                logger.info(f"🔍 [Wind详细日志] 结束日期: '{end_date}'")

            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                logger.info(f"🔍 [Wind详细日志] 开始日期为空，设置为一年前: {start_date}")
            else:
                logger.info(f"🔍 [Wind详细日志] 开始日期: '{start_date}'")

            logger.info(f"🔄 从Wind获取{wind_code}数据 ({start_date} 到 {end_date})...")
            logger.info(f"🔍 [股票代码追踪] 调用 Wind API wsd，传入参数: codes='{wind_code}', start='{start_date}', end='{end_date}'")

            # 记录API调用前的状态
            api_start_time = time.time()
            logger.info(f"🔍 [Wind详细日志] API调用开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

            # 获取日线数据
            try:
                wind_data = w.wsd(
                    codes=wind_code,
                    fields="open,high,low,close,pre_close,chg,pct_chg,volume,amt",
                    beginTime=start_date,
                    endTime=end_date,
                    options="unit=1;Fill=Previous"
                )
                api_duration = time.time() - api_start_time
                logger.info(f"🔍 [Wind详细日志] API调用完成，耗时: {api_duration:.3f}秒")

            except Exception as api_error:
                api_duration = time.time() - api_start_time
                logger.error(f"❌ [Wind详细日志] API调用异常，耗时: {api_duration:.3f}秒")
                logger.error(f"❌ [Wind详细日志] API异常类型: {type(api_error).__name__}")
                logger.error(f"❌ [Wind详细日志] API异常信息: {str(api_error)}")
                raise api_error

            # 检查Wind API返回状态
            if wind_data.ErrorCode != 0:
                logger.error(f"❌ Wind返回错误，错误代码: {wind_data.ErrorCode}")
                return pd.DataFrame()

            # 详细记录返回数据的信息
            logger.info(f"🔍 [Wind详细日志] 返回数据类型: {type(wind_data)}")
            logger.info(f"🔍 [Wind详细日志] ErrorCode: {wind_data.ErrorCode}")

            if wind_data.Data is not None and len(wind_data.Data) > 0:
                # 将Wind数据转换为DataFrame格式
                data = pd.DataFrame(wind_data.Data).T  # 转置数据
                data.columns = wind_data.Fields  # 设置列名
                data.index = wind_data.Times  # 设置时间索引
                data.index.name = 'trade_date'
                
                # 标准化列名以保持与Tushare接口一致
                column_mapping = {
                    'OPEN': 'open',
                    'HIGH': 'high', 
                    'LOW': 'low',
                    'CLOSE': 'close',
                    'PRE_CLOSE': 'pre_close',
                    'CHG': 'change',
                    'PCT_CHG': 'pct_chg',
                    'VOLUME': 'vol',
                    'AMT': 'amount'
                }
                
                # 重命名列
                for old_col, new_col in column_mapping.items():
                    if old_col in data.columns:
                        data[new_col] = data[old_col]
                
                # 添加ts_code列
                data['ts_code'] = wind_code
                
                # 重置索引，将日期转为列
                data.reset_index(inplace=True)
                
                logger.info(f"🔍 [Wind详细日志] 数据是否为空: {data.empty}")
                if not data.empty:
                    logger.info(f"🔍 [Wind详细日志] 数据列名: {list(data.columns)}")
                    logger.info(f"🔍 [Wind详细日志] 数据形状: {data.shape}")
                    if 'trade_date' in data.columns:
                        date_range = f"{data['trade_date'].min()} 到 {data['trade_date'].max()}"
                        logger.info(f"🔍 [Wind详细日志] 数据日期范围: {date_range}")

                logger.info(f"✅ 获取{wind_code}数据成功: {len(data)}条")

                # 缓存数据
                if self.enable_cache and self.cache_manager:
                    try:
                        logger.info(f"🔍 [Wind详细日志] 开始缓存数据...")
                        cache_key = self.cache_manager.save_stock_data(
                            symbol=symbol,
                            data=data,
                            data_source="wind"
                        )
                        logger.info(f"💾 A股历史数据已缓存: {symbol} (wind) -> {cache_key}")
                        logger.info(f"🔍 [Wind详细日志] 数据缓存完成")
                    except Exception as cache_error:
                        logger.error(f"⚠️ 缓存保存失败: {cache_error}")
                        logger.error(f"⚠️ [Wind详细日志] 缓存异常类型: {type(cache_error).__name__}")

                logger.info(f"🔍 [Wind详细日志] get_stock_daily 执行成功，返回数据")
                return data
            else:
                logger.warning(f"⚠️ Wind返回空数据: {wind_code}")
                logger.warning(f"⚠️ [Wind详细日志] 空数据详情: Data={wind_data.Data}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"❌ 获取{symbol}数据失败: {e}")
            logger.error(f"❌ [Wind详细日志] 异常类型: {type(e).__name__}")
            logger.error(f"❌ [Wind详细日志] 异常信息: {str(e)}")
            import traceback
            logger.error(f"❌ [Wind详细日志] 异常堆栈: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def get_stock_info(self, symbol: str) -> Dict:
        """
        获取股票基本信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            Dict: 股票基本信息
        """
        # 确保Wind连接有效
        if not self._ensure_connection():
            logger.error(f"❌ Wind连接不可用，无法获取{symbol}的股票信息")
            return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'wind_error'}

        try:
            # 标准化股票代码
            logger.info(f"🔍 [股票代码追踪] get_stock_info 调用 _normalize_symbol，传入参数: '{symbol}'")
            wind_code = self._normalize_symbol(symbol)
            logger.info(f"🔍 [股票代码追踪] _normalize_symbol 返回结果: '{wind_code}'")

            logger.info(f"🔍 [股票代码追踪] 调用 Wind API wsd，传入参数: codes='{wind_code}'")

            # 获取股票基本信息
            wind_data = w.wsd(
                codes=wind_code,
                fields="trade_code,sec_name,province,wicsname2024,mkt,ipo_date",
                beginTime="2024-01-01",
                endTime="2024-01-02",
                options=""
            )

            logger.info(f"🔍 [股票代码追踪] Wind API wsd 返回ErrorCode: {wind_data.ErrorCode}")

            if wind_data.ErrorCode == 0 and wind_data.Data is not None:
                # 将Wind数据转换为字典格式
                df = pd.DataFrame(wind_data.Data).T
                df.columns = wind_data.Fields
                
                if not df.empty:
                    row = df.iloc[0]
                    result = {
                        'symbol': symbol,
                        'code': row.get('TRADE_CODE', symbol),
                        'name': row.get('SEC_NAME', f'股票{symbol}'),
                        'area': row.get('PROVINCE', '未知'),
                        'industry': row.get('WICSNAME2024', '未知'),
                        'market': row.get('MKT', '未知'),
                        'list_date': row.get('IPO_DATE', '未知'),
                        'source': 'wind'
                    }
                    
                    logger.info(f"🔍 [股票代码追踪] 返回数据内容: {[result]}")
                    return result
            
            # 如果获取失败，返回默认信息
            logger.warning(f"⚠️ Wind未能获取{symbol}的基本信息")
            return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'wind'}

        except Exception as e:
            logger.error(f"❌ 获取{symbol}股票信息失败: {e}")
            return {'symbol': symbol, 'name': f'股票{symbol}', 'source': 'wind_error'}
    
    def get_financial_data(self, symbol: str, period: str = "20241231") -> Dict:
        """
        获取财务数据
        
        Args:
            symbol: 股票代码
            period: 报告期（YYYYMMDD）
            
        Returns:
            Dict: 财务数据字典
        """
        # 确保Wind连接有效
        if not self._ensure_connection():
            logger.error(f"❌ Wind连接不可用，无法获取{symbol}的财务数据")
            return {}

        try:
            financials = {}
            
            # 标准化股票代码
            wind_code = self._normalize_symbol(symbol)
            
            # 获取当前日期作为交易日期
            current_date = datetime.now().strftime('%Y%m%d')
            
            # 获取资产负债表
            try:
                balance_sheet = w.wss(
                    codes=wind_code,
                    fields="trade_code,wicsname2024,tot_assets,tot_liab,wgsd_stkhldrs_eq",
                    options=f"tradeDate={current_date};industryType=2;unit=1;rptDate={period};rptType=1;ShowBlank=0"
                )
                
                if balance_sheet.ErrorCode == 0 and balance_sheet.Data is not None:
                    df_balance = pd.DataFrame(balance_sheet.Data).T
                    df_balance.columns = balance_sheet.Fields
                    financials['balance_sheet'] = df_balance.to_dict('records')
                else:
                    logger.error(f"⚠️ 获取资产负债表失败，错误代码: {balance_sheet.ErrorCode}")
                    financials['balance_sheet'] = []
            except Exception as e:
                logger.error(f"⚠️ 获取资产负债表失败: {e}")
                financials['balance_sheet'] = []
            
            # 获取利润表
            try:
                income_statement = w.wss(
                    codes=wind_code,
                    fields="trade_code,wicsname2024,tot_oper_rev,tot_oper_cost,opprofit,tot_profit,net_profit_is",
                    options=f"tradeDate={current_date};industryType=2;unit=1;rptDate={period};rptType=1;ShowBlank=0"
                )
                
                if income_statement.ErrorCode == 0 and income_statement.Data is not None:
                    df_income = pd.DataFrame(income_statement.Data).T
                    df_income.columns = income_statement.Fields
                    financials['income_statement'] = df_income.to_dict('records')
                else:
                    logger.error(f"⚠️ 获取利润表失败，错误代码: {income_statement.ErrorCode}")
                    financials['income_statement'] = []
            except Exception as e:
                logger.error(f"⚠️ 获取利润表失败: {e}")
                financials['income_statement'] = []
            
            # 获取现金流量表
            try:
                cash_flow = w.wss(
                    codes=wind_code,
                    fields="trade_code,wicsname2024,net_profit_cs,fin_exp_cs,cash_recp_sg_and_rs,cash_pay_goods_purch_serv_rec",
                    options=f"tradeDate={current_date};industryType=2;unit=1;rptDate={period};rptType=1;ShowBlank=0"
                )
                
                if cash_flow.ErrorCode == 0 and cash_flow.Data is not None:
                    df_cashflow = pd.DataFrame(cash_flow.Data).T
                    df_cashflow.columns = cash_flow.Fields
                    financials['cash_flow'] = df_cashflow.to_dict('records')
                else:
                    logger.error(f"⚠️ 获取现金流量表失败，错误代码: {cash_flow.ErrorCode}")
                    financials['cash_flow'] = []
            except Exception as e:
                logger.error(f"⚠️ 获取现金流量表失败: {e}")
                financials['cash_flow'] = []
            
            return financials
            
        except Exception as e:
            logger.error(f"❌ 获取{symbol}财务数据失败: {e}")
            return {}
    
    def _normalize_symbol(self, symbol: str) -> str:
        """
        标准化股票代码为Wind格式

        Args:
            symbol: 原始股票代码

        Returns:
            str: Wind格式的股票代码
        """
        # 添加详细的股票代码追踪日志
        logger.info(f"🔍 [股票代码追踪] _normalize_symbol 接收到的原始股票代码: '{symbol}' (类型: {type(symbol)})")
        logger.info(f"🔍 [股票代码追踪] 股票代码长度: {len(str(symbol))}")
        logger.info(f"🔍 [股票代码追踪] 股票代码字符: {list(str(symbol))}")

        original_symbol = symbol

        # 移除可能的前缀
        symbol = symbol.replace('sh.', '').replace('sz.', '')
        if symbol != original_symbol:
            logger.info(f"🔍 [股票代码追踪] 移除前缀后: '{original_symbol}' -> '{symbol}'")

        # 如果已经是Wind格式（包含.），直接返回
        if '.' in symbol:
            logger.info(f"🔍 [股票代码追踪] 已经是Wind格式，直接返回: '{symbol}'")
            return symbol

        # 根据代码判断交易所（Wind格式）
        if symbol.startswith('6'):
            result = f"{symbol}.SH"  # 上海证券交易所
            logger.info(f"🔍 [股票代码追踪] 上海证券交易所: '{symbol}' -> '{result}'")
            return result
        elif symbol.startswith(('0', '3')):
            result = f"{symbol}.SZ"  # 深圳证券交易所
            logger.info(f"🔍 [股票代码追踪] 深圳证券交易所: '{symbol}' -> '{result}'")
            return result
        elif symbol.startswith('8'):
            result = f"{symbol}.BJ"  # 北京证券交易所
            logger.info(f"🔍 [股票代码追踪] 北京证券交易所: '{symbol}' -> '{result}'")
            return result
        else:
            # 默认深圳
            result = f"{symbol}.SZ"
            logger.info(f"🔍 [股票代码追踪] 默认深圳证券交易所: '{symbol}' -> '{result}'")
            return result
    
    def search_stocks(self, keyword: str) -> pd.DataFrame:
        """
        搜索股票
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            DataFrame: 搜索结果
        """
        # 确保Wind连接有效
        if not self._ensure_connection():
            logger.error("❌ Wind连接不可用，无法进行股票搜索")
            return pd.DataFrame()

        try:
            stock_list = self.get_stock_list()
            
            if stock_list.empty:
                return pd.DataFrame()
            
            # 按名称和代码搜索
            mask = (
                stock_list['name'].str.contains(keyword, na=False) |
                stock_list['symbol'].str.contains(keyword, na=False) |
                stock_list['ts_code'].str.contains(keyword, na=False)
            )
            
            results = stock_list[mask]
            logger.debug(f"🔍 搜索'{keyword}'找到{len(results)}只股票")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 搜索股票失败: {e}")
            return pd.DataFrame()


# 全局提供器实例
_wind_provider = None

def get_wind_provider() -> WindProvider:
    """获取全局Wind提供器实例"""
    global _wind_provider
    if _wind_provider is None:
        _wind_provider = WindProvider()
    return _wind_provider


def get_china_stock_data_wind(symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    获取中国股票数据的便捷函数（Wind数据源）
    
    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        DataFrame: 股票数据
    """
    provider = get_wind_provider()
    return provider.get_stock_daily(symbol, start_date, end_date)


def get_china_stock_info_wind(symbol: str) -> Dict:
    """
    获取中国股票信息的便捷函数（Wind数据源）
    
    Args:
        symbol: 股票代码
        
    Returns:
        Dict: 股票信息
    """
    provider = get_wind_provider()
    return provider.get_stock_info(symbol) 