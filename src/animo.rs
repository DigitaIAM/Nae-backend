use crate::error::DBError;
use crate::{Memory, RocksDB};
use crate::memory::{ChangeTransformation, ID, IDS, Transformation, TransformationKey, Value};

// Report
//           | open       | in         | out        | close      |
//           | qty | cost | qty | cost | qty | cost | qty | cost |
// store     |  -  |  +   |  -  |  +   |  -  |  +   |  -  |  +   |
//  goods    |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//   docs    |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//    rec?   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |

// ledger
trait OrderedCalculator {

    // goods = uom(kg, liter)

    // operation types = In(qty(number, uom), cost(number, currency)), Out(qty(..), cost(..))

    // key-value = key(store + goods + date) = value((context, operation_qty, operation_cost) + (balance_qty, balance_cost))

    // current.balance_qty = prev.balance_qty + current.operation_qty
    // current.balance_cost = prev.balance_cost + current.operation_cost

    // current.operation_cost = current.operation_qty * (prev.operation_cost / prev.operation_qty)

}

trait Calc {
    fn eval(&self, db: RocksDB, input_context: IDS, output_context: IDS) -> Result<Transformation, DBError>;
}

struct Multiply {
    input: Vec<ID>,
    output: ID,
}

impl Calc for Multiply {
    fn eval(&self, db: RocksDB, input_context: IDS, output_context: IDS) -> Result<Transformation, DBError> {
        let input_keys = self.input.iter()
            .map(|what| TransformationKey { context: input_context.clone(), what: what.clone() })
            .collect::<Vec<TransformationKey>>();

        let mut records = db.query(input_keys)?;

        let multiplier = records.remove(0).into.number_or_err()?;
        let multiplicand = records.remove(0).into.number_or_err()?;

        let product = multiplier * multiplicand;

        Ok(Transformation {
            context: output_context,
            what: self.output.clone(),
            into: Value::Number(product)
        })
    }
}

struct Animo {
    functions: Vec<(IDS, Box<dyn Calc>)>,
}

trait Handler {
    fn mutation(change: ChangeTransformation);
}

impl Handler for Animo {
    fn mutation(change: ChangeTransformation) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{Transformation, Value};
    use crate::RocksDB;

    #[actix_web::test]
    async fn test_animo() {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        env_logger::init();

        let db: RocksDB = Memory::init("./data/tests").unwrap();

        let cost = Multiply {
            input: vec!["qty".into(), "price".into()],
            output: "cost".into()
        };

        let memory = vec![
            Transformation {
                context: vec!["doc1".into()].into(),
                what: "goods".into(),
                into: Value::ID("g1".into()),
            },
            Transformation {
                context: vec!["doc1".into()].into(),
                what: "qty".into(),
                into: Value::Number(10.into()),
            },
            Transformation {
                context: vec!["doc1".into()].into(),
                what: "price".into(),
                into: Value::Number(5.into()),
            }
        ];

        // register [date, goods, qty, cost] ... cost > look up = nothing > calc cost?

        // doc1 > (doc1 price) currency > USD | doc1 price uom > kg | doc1 price number

        // doc1 price          = ID0
        // doc1 price currency = ID1 = ID0 + currency
        // doc1 price USD      = ID2 = ID0 + USD

        // ID0 = currency > USD
    }
}