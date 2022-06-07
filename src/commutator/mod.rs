use actix::prelude::*;
use crate::animo::memory::ChangeTransformation;
use crate::AnimoDB;

pub(crate) struct Commutator {
    db: AnimoDB,
}

impl Commutator {
    pub(crate) fn new(db: AnimoDB) -> Commutator {
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