from dagster import ConfigurableResource


class GetExtractPathResource(ConfigurableResource):
    campaigns: str
    customers: str
    products: str
    transactions: str


    def get_campaigns(self) -> str:
        return self.campaigns

    def get_customers(self) -> str:
        return self.customers

    def get_products(self) -> str:
        return self.products

    def get_transactions(self) -> str:
        return self.transactions

