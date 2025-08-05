import re
import pandas as pd
from .base import Base


class AliPay(Base):
    def read_csv(self):
        """Read an Alipay csv into a pandas DataFrame"""
        # The first 24 lines are headers of the table
        df = pd.read_csv(
            self.csv_path, encoding="gb18030", skiprows=24, index_col=False
        )
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x).iloc[::-1, :-1]
        self.df = df.fillna(value="")
        self.df = self.df[self.df["金额"] != 0]

    def fix_method(self):
        def fix_func(row: pd.Series) -> pd.Series:
            """Fix method"""
            if row["收/付款方式"] == "":
                if row["收/支"] == "收入" or re.search(
                    "基础软件服务费", row["商品说明"]
                ):
                    row["收/付款方式"] = "账户余额"
                elif row["商品说明"] == "退款-亲情卡":
                    order_id = row["交易订单号"].split("_")[0]
                    row["收/付款方式"] = self.df.loc[
                        self.df["交易订单号"] == order_id, "收/付款方式"
                    ].iloc[0]
                else:
                    raise ValueError(f"{row['商品说明']}收/付款方式缺失")
            elif "&" in row["收/付款方式"]:
                exist_methods = []
                for method in row["收/付款方式"].split("&"):
                    if method in self.la_account_map:
                        exist_methods.append(method)
                    else:
                        print(f"{method} not exist")
                if not exist_methods:
                    raise ValueError(f"收/付款方式{row['收/付款方式']}缺失账户")
                elif len(exist_methods) > 1:
                    raise ValueError(
                        f"复杂模式的收/付款方式{row['收/付款方式']}暂不支持"
                    )
                else:
                    row["收/付款方式"] = exist_methods[0]
            return row

        self.df = self.df.apply(fix_func, axis=1)

    def fix_noflow(self):
        def fix_func(row: pd.Series):
            if row["收/支"] == "不计收支":
                if row["交易状态"] == "还款成功" or re.search(
                    "车险|余额宝.*转入|账户安全险|蚂蚁财富.*买入", row["商品说明"]
                ):
                    row["收/支"] = "支出"
                else:
                    row["收/支"] = "收入"
            return row

        self.df = self.df.apply(fix_func, axis=1)

    def create_format_df(self):
        df = pd.DataFrame()
        df["description"] = (
            self.df["交易分类"] + " " + self.df["交易对方"] + " " + self.df["商品说明"]
        )
        df["post_date"] = pd.to_datetime(self.df["交易时间"]).dt.date
        df["amount"] = self.df.apply(
            lambda row: -row["金额"] if row["收/支"] == "支出" else row["金额"], axis=1
        )
        df["refund"] = self.df["交易状态"] == "退款成功"
        df["to"] = self.df["收/付款方式"]
        self.df = df

    def map_transfer_account(self):
        def map_func(row: pd.Series):
            """Map the transfer account from description"""
            row["transfer"] = ""
            if row["amount"] > 0 and not row["refund"]:
                for pattern, account in self.lai_account_map.items():
                    if re.search(pattern, row["description"]):
                        row["transfer"] = account
                        break
            else:
                for pattern, account in self.lae_account_map.items():
                    if re.search(pattern, row["description"]):
                        row["transfer"] = account
                        break
            if not row["transfer"]:
                raise ValueError(f"{row['description']}找不到对应的账户")
            return row

        self.df = self.df.apply(map_func, axis=1)

    def clearup(self):
        self.df = self.df.drop(columns=["refund", "to"])
