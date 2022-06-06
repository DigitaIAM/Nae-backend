use actix::prelude::*;
use crate::memory::ChangeTransformation;
use crate::RocksDB;

pub(crate) struct Commutator {
    db: RocksDB,
}

impl Commutator {
    pub(crate) fn new(db: RocksDB) -> Commutator {
        Commutator {
            db,
            // sessions: HashMap::new(),
            // rooms: HashMap::new(),
        }
    }
}

impl Actor for Commutator {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "String")]
struct Mutation(ChangeTransformation);

impl Handler<Mutation> for Commutator {
    type Result = String;

    fn handle(&mut self, msg: Mutation, ctx: &mut Self::Context) -> Self::Result {
        todo!()
    }
}

#[derive(Message)]
#[rtype(result = "String")]
struct Query(String);

impl Handler<Query> for Commutator {
    type Result = String;

    fn handle(&mut self, msg: Query, ctx: &mut Self::Context) -> Self::Result {
        todo!()
    }
}