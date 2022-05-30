use serde::{Deserialize, Serialize};

// Assets, Liabilities, Equity (or Capital), Income (or Revenue) and Expenses
enum AccountType {
    Active,
    Passive
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OpAccounts {
    Debit(Money),
    Credit(Money),
}