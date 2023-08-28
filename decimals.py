#!/usr/bin/env python3

from textwrap import dedent

settings = []

for p in range(8, 20):
    settings.append((p, 2))

for p in range(9, 25):
    settings.append((p, 4))

for p in range(18, 25):
    settings.append((p, 6))


for t in settings:
    (precision, scale) = t
    ddl = dedent(f"""
        use iceberg.decimals;
        create table d{precision}p{scale} (i int, j bigint, d decimal({precision}, {scale}));

        insert into d{precision}p{scale}
        select i, j,
            pow(10, {precision} - {scale} - 8) - 1 + (j % pow(10, {precision} - {scale} - 1)) + cast(i as decimal({precision}, {scale}))/9973
        from
            unnest(sequence(1, 10000)) as t1(i)
            cross join unnest(sequence(i*1000, i*1000+9999)) as t2(j)
        ;
        alter table d{precision}p{scale} execute optimize;
    """)
    with open(f"init-{precision}-{scale}.sql", 'wt') as f:
        print(ddl, file=f)

    with open(f"optimize-{precision}-{scale}.sql", 'wt') as f:
        sql = dedent(f"""
            use iceberg.decimals;
            set session iceberg.remove_orphan_files_min_retention = '30s';
            set session iceberg.expire_snapshots_min_retention = '30s';
            alter table d{precision}p{scale} execute expire_snapshots(retention_threshold => '31s');
            alter table d{precision}p{scale} execute remove_orphan_files(retention_threshold => '31s');
        """)
        print(sql, file=f)

    with open(f"query-{precision}-{scale}.sh", 'wt', newline='\n') as f:
        sql = "use iceberg.decimals;"
        for i in range(0, 12):
            sql += f"""select 'decimal({precision},{scale})', sum(d) from d{precision}p{scale};"""
        script = dedent(f"""\
            #!/usr/bin/env bash
            trino --progress=true --execute="{sql}" |& grep -E -o '^[0-9.]+ ' \\
            | sort -n \\
            | head -n 11 \\
            | tail -n 10 \\
            | perl -M'List::Util qw(sum)' -an0E 'say join(",", ({precision}, {scale}, sum(@F)/@F))'
        """)
        print(script, file=f)
 