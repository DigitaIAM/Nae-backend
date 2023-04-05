use crate::commutator::Application;

pub(crate) fn import(app: &Application) {
    unimplemented!()
}

pub(crate) fn report(app: &Application) {
    unimplemented!()
}

fn load() -> Vec<(usize, String, String)> {
    let text_file = "utf8_dbo.GOOD.Table.sql";
    let file = File::open(text_file).unwrap();

    let mut search_id = 0;
    let mut catalog: Vec<(usize, String, String)> = vec![];

    BufReader::new(file)
        .lines()
        .map(|l| l.unwrap())
        .filter(|l| l.starts_with("INSERT"))
        .map(|l| l[398..].to_string())
        .map(|l| {
            let name = l.split("N'").nth(1).unwrap();
            let manufacturer = l.split("N'").nth(1).unwrap();
            (
                name[0..name.len() - 3].to_owned(),
                manufacturer[0..manufacturer.len() - 3].to_owned(),
            )
        })
        .for_each(|(name, manufacturer)| {
            catalog.push((search_id, name.to_string(), manufacturer.to_string()));
            search_id += 1;
        });

    catalog
}
