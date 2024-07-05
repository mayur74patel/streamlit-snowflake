CREATE SCHEMA RAW;
CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = JSON STRIP_OUTER_ARRAY=true;
--LS @STG_RAW;

--stp-1
  select * from table(
infer_schema(
location=>'@STG_RAW/1441136',
file_format => 'my_json_format'
)
  );

  
-- select t.$1. from @STG_RAW/1441136
--  (file_format => 'my_json_format' ) t ;

--stp-2
CREATE OR REPLACE TABLE RAW_T20_DATA AS 
select replace(METADATA$FILENAME,'.json','') id, $1:info::OBJECT info,$1:innings::ARRAY innings,$1:meta::OBJECT meta
from @STG_RAW
  (file_format => 'my_json_format' ) t ;

 --STP-2
 CREATE OR REPLACE TABLE RAW_T20_DATA_INFO AS 
SELECT ID,META:created::string match_date,info FROM RAW_T20_DATA;

 CREATE OR REPLACE TABLE RAW_T20_DATA_INNINGS AS 
SELECT ID,META:created::string match_date,innings FROM RAW_T20_DATA;

--STP-3
CREATE OR REPLACE TABLE STG_RAW_T20_DATA_INFO AS 
SELECT id,match_date meta_date,
info:balls_per_over balls_per_over,
info:city::string city,
info:dates dates,
info:event event,
info:gender::string gender,
info:match_type::string match_type ,
info:match_type_number::string match_type_number,
info:officials officials,
info:outcome outcome,
info:overs overs,
info:player_of_match player_of_match,
info:players players,
info:registry registry,
info:season::string season,
info:team_type::string team_type,
info:teams[0]::string team1,
info:teams[1]::string team2,
info:toss toss,
info:venue::string venue
FROM RAW_T20_DATA_INFO; where id='1019979';

CREATE OR REPLACE TABLE T20_EVENT AS 
SELECT ID,meta_date,EVENT:name::string name,
EVENT:stage::string stage,
EVENT:match_number::string match_number,
EVENT:group::string "GROUP"
FROM STG_RAW_T20_DATA_INFO;


CREATE OR REPLACE TABLE T20_OFFICIALS AS 
SELECT ID,META_DATE,
OFFICIALS:reserve_umpires[0]::STRING reserve_umpires, 
OFFICIALS:tv_umpires[0]::STRING tv_umpires, 
OFFICIALS:umpires umpires, 
OFFICIALS:match_referees[0]::STRING match_referees
FROM STG_RAW_T20_DATA_INFO ;

CREATE OR REPLACE TABLE T20_OUTCOME AS
SELECT ID,META_DATE,
OUTCOME:method::STRING method, 
OUTCOME:result::STRING result, 
OUTCOME:by "BY", 
OUTCOME:bowl_out::STRING bowl_out,
OUTCOME:eliminator::STRING eliminator,
OUTCOME:winner::STRING winner
FROM STG_RAW_T20_DATA_INFO;


CREATE OR REPLACE TABLE T20_PLAYERS AS
SELECT ID,META_DATE,PLAYERS,REGISTRY
FROM STG_RAW_T20_DATA_INFO;


CREATE OR REPLACE TABLE T20_TOSS AS
SELECT ID,META_DATE,
TOSS:decision::STRING decision,
TOSS:winner::STRING winner
FROM STG_RAW_T20_DATA_INFO;

CREATE OR REPLACE TABLE T20_DATA_INFO AS
SELECT id, meta_date,
balls_per_over,
city,
dates,
gender,
match_type ,
match_type_number,
overs,
player_of_match,
season,
team_type,
team1,
team2,
venue
FROM STG_RAW_T20_DATA_INFO;


CREATE OR REPLACE TABLE T20_DELIVERIES AS 
SELECT id,match_date meta_date,INNINGS[0]:team::string team,a1.value:over over,a2.index ball,
a2.value:batter::string batter,
a2.value:bowler::string bowler,
a2.value:non_striker::string non_striker,
a2.value:runs:batter batter_runs,
a2.value:runs:extras extras,
a2.value:runs:total total,
a2.value:wickets::string wickets,
a2.value:wickets[0]:kind::string wickets_kind,
a2.value:wickets[0]:player_out::string wickets_player_out,
a2.value:extras:legbyes legbyes,
a2.value:extras:noballs noballs,
a2.value:extras:wides wides
FROM RAW_T20_DATA_INNINGS ,
lateral flatten(input => INNINGS[0]:overs) as a1,
lateral flatten(input => a1.value:deliveries) as a2
UNION ALL
SELECT id,match_date meta_date,INNINGS[1]:team::string team,a1.value:over over,a2.index ball,
a2.value:batter::string batter,
a2.value:bowler::string bowler,
a2.value:non_striker::string non_striker,
a2.value:runs:batter batter_runs,
a2.value:runs:extras extras,
a2.value:runs:total total,
a2.value:wickets::string wickets,
a2.value:wickets[0]:kind::string wickets_kind,
a2.value:wickets[0]:player_out::string wickets_player_out,
a2.value:extras:legbyes legbyes,
a2.value:extras:noballs noballs,
a2.value:extras:wides wides
FROM RAW_T20_DATA_INNINGS ,
lateral flatten(input => INNINGS[1]:overs) as a1,
lateral flatten(input => a1.value:deliveries) as a2
;

SELECT over,COUNT(*) FROM T20_DELIVERIES where id='1007657'
AND TEAM='India' group by over;
SELECT meta_date,year(to_date(meta_date,'yyyy-mm-dd')) FROM T20_EVENT ;

select * from CRICKET.RAW.T20_OUTCOME where "BY" is  null;

CREATE OR REPLACE VIEW VW_T20_RESULT AS 
select ID,META_DATE,object_keys("BY")[0]::string type ,
case when type='wickets' then WINNER ||' Won by '|| "BY":wickets::string ||' ' ||type 
when type='runs' then   WINNER ||' won by '|| "BY":runs::string ||' ' ||type 
when type is null and ELIMINATOR is not null then 'match '||RESULT::string || ' and '||ELIMINATOR||' Eliminated from tournament'
when type is null and BOWL_OUT is not null then 'match '||RESULT::string || ' and in bowl out '||BOWL_OUT||' won'
else RESULT::string end result_by
from CRICKET.RAW.T20_OUTCOME; where id='1438073';
SELECT * FROM VW_T20_RESULT;

SELECT * FROM T20_DATA_INFO WHERE ID='1007657';

CREATE OR REPLACE TABLE T20_DELIVERIES_BAT_SCORE AS 
SELECT ID,META_DATE,TEAM,BATTER,SUM(BATTER_RUNS) RUNS,COUNT(*)-COUNT(WIDES) BOWL,
sum(CASE when BATTER_RUNS=4 THEN 1 ELSE 0 END) AS "4s",
sum(CASE when BATTER_RUNS=6 THEN 1 ELSE 0 END) AS "6s",
CASE WHEN BOWL!=0 THEN TO_NUMBER((RUNS/BOWL)*100,10,2) ELSE 0 END AS SR
FROM T20_DELIVERIES 
GROUP BY ID,META_DATE,TEAM,BATTER;

--run out,retired out,retired not out,retired hurt,hit the ball twice,obstructing the field
--bowled,caught,stumped,caught and bowled,lbw
SELECT 'run out ('|| parse_json(to_variant(wickets))[0]:fielders[0]:name::string||','||parse_json(to_variant(wickets))[0]:fielders[1]:name::string fielders||')'

SELECT distinct wickets_kind FROM T20_DELIVERIES ;
SELECT * FROM T20_DATA_INFO WHERE ID='1074959';

CREATE OR REPLACE TABLE T20_DELIVERIES_BOWLING_SCORE AS 
SELECT ID,META_DATE,TEAM,BOWLER,COUNT(DISTINCT OVER) OVERS,SUM(TOTAL) RUNS,SUM(WIDES) WD,SUM(NOBALLS) NB,
SUM(case when WICKETS_KIND IN ('bowled','caught','stumped','caught and bowled','lbw')
THEN 1 ELSE 0 END ) W
FROM T20_DELIVERIES  GROUP BY ID,META_DATE,TEAM,BOWLER
;

create or replace table T20_DELIVERIES_WICKETS as
SELECT ID ,META_DATE,TEAM,WICKETS_KIND,WICKETS_PLAYER_OUT,BOWLER,
case when WICKETS_KIND='caught' then 'c '||PARSE_JSON(TO_VARIANT(WICKETS))[0]:fielders[0]:name::string||' b '||BOWLER
when WICKETS_KIND='bowled' then 'b '||BOWLER
WHEN WICKETS_KIND='lbw' then 'lbw '||BOWLER
WHEN WICKETS_KIND='caught and bowled' then 'c&b '||BOWLER
WHEN WICKETS_KIND='hit wicket' then WICKETS_KIND||' b '||BOWLER
WHEN WICKETS_KIND IN ('retired out','retired not out','retired hurt','hit the ball twice','obstructing the field') THEN WICKETS_KIND
WHEN WICKETS_KIND='stumped' then 'st '||PARSE_JSON(TO_VARIANT(WICKETS))[0]:fielders[0]:name::string||' b '||BOWLER 
WHEN WICKETS_KIND='run out' and array_size(parse_json(to_variant(wickets))[0]:fielders)>1 then
    'run out ('|| parse_json(to_variant(wickets))[0]:fielders[0]:name::string||','||parse_json(to_variant(wickets))[0]:fielders[1]:name::string||')'
when  WICKETS_KIND='run out' and array_size(parse_json(to_variant(wickets))[0]:fielders)=1 then   
    'run out ('||parse_json(to_variant(wickets))[0]:fielders[0]:name::string||')'
else WICKETS_KIND
END AS STATUS
FROM T20_DELIVERIES 
WHERE WICKETS IS NOT NULL;

CREATE OR REPLACE VIEW  VW_T20_DELIVERIES_SCORECARD_BAT AS 
SELECT SC.BATTER,,STATUS FROM T20_DELIVERIES_BAT_SCORE SC
LEFT JOIN T20_DELIVERIES_WICKETS WK 
ON SC.ID=WK.ID AND SC.BATTER=WK.WICKETS_PLAYER_OUT; 
WHERE
SC.ID='1007657' AND SC.TEAM='India';

select BATTER,case when STATUS is null then 'Not Out' else STATUS END AS STATUS,RUNS,BOWL,4S,6S,SR from CRICKET.RAW.VW_T20_DELIVERIES_SCORECARD_BAT where id=1438074 and TEAM='Denmark';

SELECT BATTER,case when STATUS is null then 'Not Out' else STATUS END AS STATUS,RUNS,BOWL,4S,6S,SR FROM VW_T20_DELIVERIES_SCORECARD_BAT; WHERE
ID='1007657' AND TEAM='India';


SELECT * FROM T20_DELIVERIES_WICKETS WHERE
ID='1007657' AND TEAM='Zimbabwe';

SELECT * FROM T20_DELIVERIES_WICKETS WHERE
ID='1007657' AND TEAM='India';

SELECT ID,TEAM,BOWLER,OVERs,RUNS,NVL(WD,0) WD,NVL(NB,0) NB,W FROM T20_DELIVERIES_BOWLING_SCORE
WHERE
ID='1007657' AND TEAM='India';

SELECT TEAM,DECISION FROM T20_TOSS WHERE ID='1007657';

SELECT TEAM,ID,SUM(TOTAL),COUNT(WICKETS) FROM T20_DELIVERIES WHERE
ID='1007657' GROUP BY TEAM,ID;

CREATE OR REPLACE VIEW  VW_BATTER_ORDER AS 
with data_ball_no as (
select  ID,TEAM,BATTER,OVER,ball,row_number() over(partition by id,team order by over,ball) BALL_NO from T20_DELIVERIES  ),
min_data_ball_no as
(
select id,team,batter,min(ball_no) min_ball_no from data_ball_no group by id,team,batter)
select ID,TEAM,BATTER,dense_rank() over (partition by id,team order by min_ball_no) batter_order from min_data_ball_no
;

CREATE OR REPLACE VIEW  VW_T20_DELIVERIES_SCORECARD_BAT_ORDER AS 
SELECT A.*,B.BATTER_ORDER FROM VW_T20_DELIVERIES_SCORECARD_BAT A LEFT JOIN VW_BATTER_ORDER B
ON A.ID=B.ID AND B.TEAM=A.TEAM AND B.BATTER=A.BATTER
;
select * from VW_BATTER_ORDER WHERE
ID='1007657';

select  ID,TEAM,BOWLER,MIN(OVER) from T20_DELIVERIES WHERE ID='1007657' GROUP BY ID,TEAM,BOWLER ORDER BY 2; 

CREATE OR REPLACE  VIEW VW_BOWLING_ORDER AS 
WITH MIN_OVER AS (
select  ID,TEAM,BOWLER,MIN(OVER) min_over_no from T20_DELIVERIES   GROUP BY ID,TEAM,BOWLER
)
SELECT *,dense_rank() over (partition by id,team order by min_over_no) bowler_order FROM MIN_OVER;

CREATE OR REPLACE VIEW  VW_T20_DELIVERIES_BOWLING_SCORE_ORDER AS 
SELECT A.*,B.BOWLER_ORDER FROM T20_DELIVERIES_BOWLING_SCORE A LEFT JOIN VW_BOWLING_ORDER B
ON A.ID=B.ID AND B.TEAM=A.TEAM AND B.bowler=A.bowler;




SELECT WINNER,DECISION FROM T20_TOSS WHERE ID='1007657';

SELECT TEAM,ID,SUM(TOTAL),COUNT(WICKETS),MAX(over) FROM T20_DELIVERIES WHERE
ID='1007657' GROUP BY TEAM,ID;

with max_ball_over as (
select TEAM,ID,over,MAX(ball) max_ball
from T20_DELIVERIES where ID='1007657' GROUP BY TEAM,ID,over),
deliveries as (
select a.team,a.id,a.over,total,wickets,case when ball=max_ball then to_number(a.over+1) else a.over end as over_no from T20_DELIVERIES a left join max_ball_over b on a.team=b.team
and a.id=b.id and a.over=b.over  where a.ID='1007657')
SELECT TEAM,ID,SUM(TOTAL),COUNT(WICKETS),
MAX(over_no) FROM deliveries WHERE
ID='1007657' GROUP BY TEAM,ID;

create view VW_T20_OVERS AS
with ball_no as (
select TEAM,ID,over,ball,row_number() over (partition by id,team order by over,ball) ball_no
from T20_DELIVERIES ),
ball_max as (
select team,id,max(ball_no) max_ball_no from ball_no   group by team,id
),
over_total as ( 
select A.TEAM,A.ID,CASE WHEN over=19 and BALL+1>5 THEN '20'::STRING ELSE over::string||'.'||(ball+1)::string end as OVER_STATUS  from ball_no a,ball_max b where a.team=b.team and a.id=b.id and a.ball_no=b.max_ball_no)
SELECT *  FROM over_total
;


CREATE OR REPLACE VIEW  VW_SCORE_WICKET AS 
with ball_no as (
select TEAM,ID,over,ball,row_number() over (partition by id,team order by over,ball) ball_no
from T20_DELIVERIES ),
ball_max as (
select team,id,max(ball_no) max_ball_no from ball_no   group by team,id
),
over_total as ( 
select A.TEAM,A.ID,CASE WHEN over=19 and BALL+1>5 THEN '20'::STRING ELSE over::string||'.'||(ball+1)::string end as OVER_STATUS  from ball_no a,ball_max b where a.team=b.team and a.id=b.id and a.ball_no=b.max_ball_no),
 score_wicket as (
SELECT ID, TEAM,SUM(TOTAL)::string||'-'||COUNT(WICKETS)::string sc_wicket FROM T20_DELIVERIES  GROUP BY TEAM,ID)
select a.team,a.id,sc_wicket,over_status from over_total a,score_wicket b where a.team=b.team and a.id=b.id
;

CREATE OR REPLACE VIEW VW_T20_FALL_WICKETS AS 
SELECT ID,TEAM,LISTAGG(SUM_BALL||'-'||WICKET_NO||' ('||WICKETS_PLAYER_OUT||','||W_OV||')',',') RESULT FROM 
(
SELECT ID,TEAM,OVER,BALL,TOTAL,WICKETS_PLAYER_OUT,
CASE WHEN WICKETS_PLAYER_OUT IS NOT NULL THEN OVER||'.'||BALL ELSE NULL END AS  W_OV,
CASE WHEN WICKETS_PLAYER_OUT IS NOT NULL THEN DENSE_RANK() OVER( PARTITION BY ID,TEAM,WICKETS_PLAYER_OUT IS  NULL  ORDER BY OVER,BALL) ELSE 0 END AS WICKET_NO,
SUM(TOTAL) OVER(PARTITION BY ID,TEAM ORDER BY OVER,BALL) SUM_BALL
FROM T20_DELIVERIES ) WHERE WICKETS_PLAYER_OUT IS NOT NULL
GROUP BY ID,TEAM
;


CREATE OR REPLACE VIEW VW_T2O_PLAYER AS 
with team_player as (
select id,meta_date,key team,value player_list from T20_PLAYERS,
LATERAL FLATTEN (input=>PLAYERS))
select id,meta_date,team,value::string player_name from team_player,
LATERAL FLATTEN (input=>player_list)
;


CREATE OR REPLACE VIEW VW_PLAYET_YET_BAT AS 
SELECT A.ID,A.TEAM,LISTAGG(A.PLAYER_NAME,',') PLAYER_LIST FROM VW_T2O_PLAYER A
LEFT JOIN VW_T20_DELIVERIES_SCORECARD_BAT_ORDER B
ON A.ID=B.ID AND A.TEAM=B.TEAM AND A.PLAYER_NAME=B.BATTER
WHERE B.ID IS NULL 
GROUP BY A.TEAM,A.ID;

CREATE OR REPLACE VIEW VW_T20_EXTRAS AS 
select ID,TEAM,(NVL(SUM(LEGBYES),0)+NVL(SUM(NOBALLS),0)+NVL(SUM(WIDES),0)) || ' ( '||NVL(SUM(LEGBYES),0)||' LB,'||NVL(SUM(NOBALLS),0)||' NB,'||NVL(SUM(WIDES),0)||' WD)' EXTRAS
from T20_DELIVERIES 
GROUP BY ID,TEAM;
