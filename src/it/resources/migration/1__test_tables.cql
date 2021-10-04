create table test_data(
    id bigint,
    data text,
    PRIMARY KEY (id)
);

insert into test_data (id, data) values (0, null);
insert into test_data (id, data) values (1, 'one');
insert into test_data (id, data) values (2, 'two');
insert into test_data (id, data) values (3, 'three');

create table test_data_multiple_keys(
    id1 bigint,
    id2 int,
    data text,
    PRIMARY KEY (id1, id2)
);

insert into test_data_multiple_keys (id1, id2, data) values (1, 2, 'one-two');

create type basic_info(
    weight double,
    height text,
    datapoints frozen<set<int>>
);

create table person_attributes(
    person_id int,
    info frozen<basic_info>,
    PRIMARY KEY (person_id)
);
