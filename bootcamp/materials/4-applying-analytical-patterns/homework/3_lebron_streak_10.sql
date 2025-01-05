
with de_duped_games as (
    select distinct
        game_id,
        game_date_est
    from games
),

base_table as (
    select distinct
        gd.game_id,
        g.game_date_est,
        gd.team_id,
        gd.player_name,
        case when gd.pts > 10 then 1 else 0 end as result
    from game_details as gd
    inner join de_duped_games as g on gd.game_id = g.game_id
    where gd.player_name = 'LeBron James'
),

grouped as (
    select
        *,
        case
            when LAG(result) over (order by game_date_est) <> result
                then 1
            else 0
        end as counter
    from base_table
),

final as (
    select
        *,
        SUM(counter) over (order by game_date_est) as group_id
    from grouped
)

select
    player_name,
    MAX(streak) as max
from (
    select
        player_name,
        group_id,
        SUM(result) as streak
    from final group by 1, 2
) as temp group by 1
