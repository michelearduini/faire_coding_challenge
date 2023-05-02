import pandas
from datetime import timedelta
import numpy as np
from sklearn.linear_model import LinearRegression


def average_booked_balance_from(transactions: pandas.DataFrame,
                                accounts: pandas.DataFrame,
                                reference_timestamps: pandas.DataFrame) -> pandas.Series:
    """
    :param transactions: pandas dataframe containing the transactions from a collection of accounts
    :param accounts: pandas dataframe containing a collection of accounts together with their balance when they
        were first added to our systems.
    :param reference_timestamps: pandas dataframe with the timestamp a which to compute the average booked balance for
        each account. Different account might have different reference timestamps.
    :return:
        a pandas series where the index is a multindex containing the reference timestamp and the account id, and the
        values are the average booked balances, e.g

        index                               | value
        ('2022-01-12 23:59:59.999', 'ac_1') | 12.3
        ('2022-03-10 23:59:59.999', 'ac_2') | 26.8
    """
    # insert code to compute the average booked balance here.

    transactions['value_timestamp'] = pandas.to_datetime(transactions['value_timestamp'])
    accounts['creation_timestamp'] = pandas.to_datetime(accounts['creation_timestamp'])
    reference_timestamps['reference_timestamp_datetime'] = pandas.to_datetime(reference_timestamps['reference_timestamp'])

    daily_balances = {}

    for _, ref_row in reference_timestamps.iterrows():
        account_id = ref_row['account_id']
        ref_ts = ref_row['reference_timestamp_datetime']
        creation_ts = accounts.loc[accounts['account_id'] == account_id, 'creation_timestamp'].iloc[0]
        balance_at_creation = accounts.loc[accounts['account_id'] == account_id, 'balance_at_creation'].iloc[0]
        
        start_ts = ref_ts - timedelta(days=90)
        date_range = pandas.date_range(start_ts, ref_ts, closed='left')
        account_transactions = transactions[transactions['account_id'] == account_id]
        
        daily_balance = []
        
        for date in date_range:
            if date < creation_ts:
                daily_balance.append(np.nan)
            else:
                balance = balance_at_creation
                relevant_transactions = account_transactions[account_transactions['value_timestamp'] <= date]
                balance += relevant_transactions['amount'].sum()
                daily_balance.append(balance)

        # Get non-NaN elements and their indices
        non_nan_elements = [x for x in daily_balance if not np.isnan(x)]
        non_nan_indices = [i for i, x in enumerate(daily_balance) if not np.isnan(x)]

        if len(non_nan_elements) >= 1:

            # Fit a linear regression model on non-NaN elements and their indices
            non_nan_indices = np.array(non_nan_indices).reshape(-1, 1)
            model = LinearRegression().fit(non_nan_indices, non_nan_elements)

            # Replace NaN elements with the projected trend
            filled_data = [x if not np.isnan(x) else model.predict(np.array([[i]]))[0] for i, x in enumerate(daily_balance)]

            daily_balances[account_id] = filled_data
        else:
            daily_balances[account_id] = [balance_at_creation] * len(daily_balance)


    average_daily_balances = {}

    for account_id, daily_balance in daily_balances.items():
        average_balance = sum(daily_balance) / len(daily_balance)
        average_daily_balances[account_id] = average_balance

    dates = reference_timestamps['reference_timestamp']
    account_ids = reference_timestamps['account_id']
    index =  pandas.MultiIndex.from_arrays([dates, account_ids], names=["reference_timestamp", "account_id"])
    values = [average_daily_balances.get(id, None) for id in account_ids]

    s = pandas.Series(values, index=index)

    return s


