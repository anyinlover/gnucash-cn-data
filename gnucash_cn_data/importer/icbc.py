import linecache
import re
import pandas as pd
from .base import Base


class ICBC(Base):
    def read_csv(self):
        """Read an ICBC csv into a pandas DataFrame"""
        # Extract the card number
        card_id = linecache.getline(str(self.csv_path), 3)[4:16]
        # The first 6 lines are headers of the table, the last line is the summary line
        df = pd.read_csv(self.csv_path, skiprows=6, skipfooter=1, index_col=False, engine="python")
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x).iloc[::-1]
        df["card_id"] = card_id
        self.df = df.fillna("")

    def create_format_df(self):
        """Convert original data into Piecash format"""
        df = pd.DataFrame()
        df["description"] = (
            self.df["摘要"]
            + " "
            + self.df["交易详情"]
            + " "
            + self.df["交易场所"]
            + " "
            + self.df["对方户名"]
            + " "
            + self.df["对方账户"]
        )
        df["post_date"] = pd.to_datetime(self.df["交易日期"]).dt.date
        df["amount"] = self.df.apply(
            lambda row: float(row["记账金额(收入)"].replace(",", ""))
            if row["记账金额(收入)"]
            else -float(row["记账金额(支出)"].replace(",", "")),
            axis=1,
        )
        df["from"] = self.df["对方账户"]
        df["refund"] = self.df["摘要"].isin(["退款", "冲正"])
        df["to"] = self.df["card_id"]
        self.df = df

    def map_transfer_account(self):
        def map_func(row: pd.Series) -> pd.Series:
            """Map the transfer account from description"""
            row["transfer"] = ""
            if row["from"] in self.la_account_map:
                row["transfer"] = self.la_account_map[row["from"]]
            elif row["amount"] > 0 and not row["refund"]:
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
        self.df = self.df.drop(columns=["from", "to"])
