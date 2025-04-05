import linecache
import re
import pandas as pd
from .base import Base


class CMB(Base):
    def read_csv(self):
        """Read an CMB csv into a pandas DataFrame"""
        # Extract the card number
        card_id = linecache.getline(str(self.csv_path), 3)[15:31]
        # The first 6 lines are headers of the table, the last line is the summary line
        df = pd.read_csv(self.csv_path, skiprows=7, skipfooter=2, index_col=False, engine="python")
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x).iloc[::-1]
        df["card_id"] = card_id
        self.df = df.fillna("")

    def create_format_df(self):
        """Convert original data into Piecash format"""
        df = pd.DataFrame()
        df["description"] = (
            self.df["交易类型"]
            + " "
            + self.df["交易备注"]
        )
        df["post_date"] = pd.to_datetime(self.df["交易日期"], format='%Y%m%d').dt.date
        df["amount"] = self.df.apply(
            lambda row: float(row["收入"])
            if row["收入"]
            else -float(row["支出"]),
            axis=1,
        )
        df["to"] = self.df["card_id"]
        self.df = df

    def map_transfer_account(self):
        def map_func(row: pd.Series) -> pd.Series:
            """Map the transfer account from description"""
            row["transfer"] = ""
            if row["amount"] > 0:
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
        self.df = self.df.drop(columns=["to"])
