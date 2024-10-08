from pyspark.sql.functions import *


class PurchaseAnalytics:


    @staticmethod
    def filter_purchases(purchase_df, lower_bound = 1.0, upper_bound=10.0):
        total_spend = col("quantity") * col("priceperunit")
        filter_condition = (total_spend >= lower_bound) & (total_spend <= upper_bound)
        return purchase_df.filter(filter_condition)


    @staticmethod
    def product_total_spend(purchase_df):
        return purchase_df. \
            groupBy(purchase_df['purchaseid']). \
            agg(sum(purchase_df['quantity'] * purchase_df['priceperunit'])). \
            alias('product_total_spend')
