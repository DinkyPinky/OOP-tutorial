import pandas as pd


class SalesProcessor:
    """
    Преобразовываем данные по продажам
    Вход: датафрейм с обязательными столбцами 'date_sale', 'pieces'
    """
    def __init__(self, sales_data):
        self.sales_data = sales_data

        self.filled_data = None
        self.processed_data = None
        self.grouped_data = None

    def fill_missing_values(self):
        self.filled_data = self.sales_data.fillna(0)
        return self.filled_data

    def process_data(self):
        processed_data = self.filled_data.copy()

        processed_data['date_sale'] = pd.to_datetime(processed_data['date_sale'])
        processed_data['year'] = processed_data['date_sale'].dt.year
        processed_data['pieces_lag'] = processed_data['pieces'].shift()

        self.processed_data = processed_data
        return self.processed_data

    def group_sales(self):
        self.grouped_data = self.sales_data.groupby('year')[['sales']].sum().reset_index()
        return self.grouped_data

    def run_all(self):
        self.fill_missing_values()
        self.process_data()
        self.group_sales()