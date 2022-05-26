use crate::error::DBError;
use crate::{Memory, RocksDB};
use crate::memory::{ChangeTransformation, ID, IDS, Transformation, TransformationKey, Value};

// Report for dates
//           | open       | in         | out        | close      |
//           | qty | cost | qty | cost | qty | cost | qty | cost |
// store     |  -  |  +   |  -  |  +   |  -  |  +   |  -  |  +   |
//  goods    |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//   docs    |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//    rec?   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |

// store     |  -  |  +   |  -  |  +   |  -  |  +   |  -  |  +   |
//  docs     |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//   goods   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//    rec?   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |


// расход1 storeB копыта 1
// расход1 storeB рога   2
// расход2 storeB копыта 3

// отчет о движение
// storeB    |     | =100 |     |      |     |  =80 |     |  =20 |
//  копыта   |  5  |  100 |     |      | =4  |  =80 |  =1 |  =20 |
//   расход1 |  5  |  100 |     |      |  1  |  =20 |  =4 |  =80 |
//   расход2 |  4  |  80  |     |      |  3  |  =60 |  =1 |  =20 |

//реестр документов
// storeB    |     | =100 |     |      |     |  =80 |     |  =20 |
//  расход1  |     |  100 |     |      |     |  =20 |     |  =80 |
//   копыта  |  5  |  100 |     |      |  1  |  =20 |  =4 |  =80 |
//  расход2  |     |  80  |     |      |     |  =60 |     |  =20 |
//   копыта  |  4  |  80  |     |      | =3  |  =60 |  =1 |  =20 |

// ledger
trait OrderedCalculator {

    // goods = uom(kg, liter)

    // operation = In(qty(number, uom), cost(number, currency)), Out(qty(..), cost(..))
    // balance(qty(number, uom), cost(number, currency))

    // key-value = key(store + goods + date) = value(context, operation, balance)

    // current.balance.qty = prev.balance.qty + current.operation.qty
    // current.balance.cost = prev.balance.cost + current.operation.cost

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