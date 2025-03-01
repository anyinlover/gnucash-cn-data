import re
import pandas as pd
from .base import Base


class JD(Base):
    def read_csv(self):
        """Read an JD csv into a pandas DataFrame"""
        # The first 21 lines are headers of the table
        df = pd.read_csv(self.csv_path, skiprows=21, index_col=False)
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x).iloc[::-1, :-1]
        df.loc[:, "金额"] = pd.to_numeric(df["金额"].str.replace(r"\(.*\)", "", regex=True))
        self.df = df
        # self.df = df.fillna(value="")

    def fix_noflow(self):
        def fix_func(row: pd.Series):
            if row["收/支"] == "不计收支":
                if row["交易状态"] in ("退款成功", "转出到账") or re.search("余额提现", row["交易说明"]):
                    row["收/支"] = "收入"
                elif row["交易状态"] in ("还款成功", "交易成功") or re.search(r"白条.*还款", row["交易说明"]):
                    row["收/支"] = "支出"
                else:
                    raise ValueError(f"{row['交易时间']} {row['交易说明']} 仍存在不计支收项")
            return row

        self.df = self.df.apply(fix_func, axis=1)

    def create_format_df(self):
        df = pd.DataFrame()
        df["description"] = self.df["交易分类"] + " " + self.df["商户名称"] + " " + self.df["交易说明"]
        df["post_date"] = pd.to_datetime(self.df["交易时间"]).dt.date
        df["amount"] = self.df.apply(lambda row: -row["金额"] if row["收/支"] == "支出" else row["金额"], axis=1)
        df["refund"] = self.df["交易状态"] == "退款成功"
        df["to"] = self.df["收/付款方式"]
        self.df = df

    def map_transfer_account(self):
        """Map the transfer account from description"""

        def map_func(row: pd.Series) -> pd.Series:
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
