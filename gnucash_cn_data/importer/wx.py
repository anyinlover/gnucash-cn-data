import re
import pandas as pd
from .base import Base


class WX(Base):
    def read_csv(self):
        """Read an wechat csv into a pandas DataFrame"""
        # The first 16 lines are headers of the table
        df = pd.read_csv(self.csv_path, skiprows=16)
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x).iloc[::-1, :-1]
        self.df = df

    def fix_method(self):
        self.df["支付方式"] = self.df["支付方式"].map(lambda x: "零钱" if x == "/" else x)

    def fix_noflow(self):
        def fix_func(row: pd.Series) -> pd.Series:
            if row["收/支"] == "/":
                if row["交易类型"] == "零钱提现":
                    row["收/支"] = "收入"
                elif re.search("零钱充值|购买理财通|转入零钱通", row["交易类型"]):
                    row["收/支"] = "支出"
                else:
                    raise ValueError(f"{row['交易时间']} {row['商品']} 仍存在不计支收项")
            return row

        self.df = self.df.apply(fix_func, axis=1)

    def create_format_df(self):
        df = pd.DataFrame()
        df["description"] = self.df["交易类型"] + " " + self.df["交易对方"] + " " + self.df["商品"]
        df["post_date"] = pd.to_datetime(self.df["交易时间"]).dt.date
        df["amount"] = self.df.apply(
            lambda row: float(row["金额(元)"][1:]) if row["收/支"] == "收入" else -float(row["金额(元)"][1:]), axis=1
        )
        df["refund"] = self.df["交易类型"].str.contains("退款")
        df["to"] = self.df["支付方式"]
        self.df = df

    def map_transfer_account(self):
        def map_func(row: pd.Series):
            """Map the from account from description"""
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
