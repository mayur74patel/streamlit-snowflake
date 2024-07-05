#use this code in snowflake streamlit 
# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("CRICKET DATA")

def get_data(sql):
    return session.sql(sql).to_pandas()

def color(text):
     st.markdown(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{text}</p>', unsafe_allow_html=True)

sql = f"SELECT DISTINCT year(to_date(meta_date,'yyyy-mm-dd')) year  FROM CRICKET.RAW.T20_EVENT order by 1 desc "
data=get_data(sql)

year_option = st.selectbox(
    "Choose YEAR",
    data)

sql = f"SELECT DISTINCT NAME  FROM CRICKET.RAW.T20_EVENT WHERE year(to_date(meta_date,'yyyy-mm-dd'))='{year_option}'"
data=get_data(sql)

option = st.selectbox(
    "Choose Event",
    data)
option=option.replace("'","''")
sql =f"select id,team1,team2 from CRICKET.RAW.T20_DATA_INFO  where id in (select id from CRICKET.RAW.T20_EVENT where name =\'{option}\' and year(to_date(meta_date,'yyyy-mm-dd'))='{year_option}')"
data=get_data(sql)

col1, col2 = st.columns(2)
with col1:
    team1=st.selectbox('TEAM1',data.TEAM1.unique())

with col2:
    team2=st.selectbox('TEAM2',data.TEAM2.unique())

t1=[team1]
t2=[team2]


match=data[data['TEAM1'].isin(t1) & data['TEAM2'].isin(t2)]

if match.empty:
    st.write("no data")
else:
    id=match['ID'].to_list()[0]
    sql=f"select result_by from CRICKET.RAW.VW_T20_RESULT where id={id}"
    data=get_data(sql)
    res=data['RESULT_BY'].to_list()[0]
    color(res)
    
    
    sql=f"select DISTINCT TEAM from CRICKET.RAW.VW_T20_DELIVERIES_SCORECARD_BAT where id={id}"
    data=get_data(sql)
    TEAM1=data['TEAM'].to_list()[0]
    TEAM2=data['TEAM'].to_list()[1]

    sql=f"SELECT WINNER,DECISION FROM T20_TOSS WHERE ID={id}"
    data=get_data(sql)
    TEAM=data['WINNER'].to_list()[0]
    DECISION=data['DECISION'].to_list()[0]
    res='Toss: '+TEAM+',elected '+DECISION+' first'
    st.write(f':blue[{res}]')

    if DECISION=='bat' and TEAM2==TEAM:
        TEAM1,TEAM2=TEAM2,TEAM1
    elif DECISION!='bat' and TEAM1==TEAM:
        TEAM1,TEAM2=TEAM2,TEAM1
    else:
        pass

    sql=f"SELECT SC_WICKET,OVER_STATUS FROM VW_SCORE_WICKET where id={id} and TEAM='{TEAM1}'"
    data=get_data(sql)
    score=data['SC_WICKET'].to_list()[0]+'('+data['OVER_STATUS'].to_list()[0]+')'
    col1,_, col2 = st.columns(3)
    with col1:
        st.write(TEAM1,' Innings')
    with col2:
        st.write(score)    
    
    sql=f"select BATTER,case when STATUS is null then 'Not Out' else STATUS END AS STATUS,RUNS::string RUNS,BOWL,\"4s\",\"6s\",SR from CRICKET.RAW.VW_T20_DELIVERIES_SCORECARD_BAT_ORDER where id={id} and TEAM='{TEAM1}' ORDER BY BATTER_ORDER"
    data=get_data(sql)
    st.dataframe(data,hide_index=True)
    
    sql=f"SELECT EXTRAS FROM VW_T20_EXTRAS where id={id} and TEAM='{TEAM1}'"
    data=get_data(sql)
    col1,_, col2 = st.columns(3)
    with col1:
        st.write('Extras')
    with col2:
        st.write(data['EXTRAS'].to_list()[0]) 
    sql=f"SELECT BOWLER,OVERS,RUNS::string RUNS,NVL(WD,0)::string  WD,NVL(NB,0)::string  NB,W FROM T20_DELIVERIES_BOWLING_SCORE where id={id} and TEAM='{TEAM1}'"
    data=get_data(sql)
    st.dataframe(data,hide_index=True)

    sql=f"SELECT PLAYER_LIST FROM VW_PLAYET_YET_BAT where id={id} and TEAM='{TEAM1}'"
    data=get_data(sql)
    if data['PLAYER_LIST'].to_list().__len__()>0:
        st.write('Did not Bat yet')
        st.write(data['PLAYER_LIST'].to_list()[0])

    sql=f"SELECT RESULT FROM VW_T20_FALL_WICKETS where id={id} and TEAM='{TEAM1}'"
    data=get_data(sql)
    if data['RESULT'].to_list().__len__()>0:
        st.write('Fall of Wickets')
        st.write(data['RESULT'].to_list()[0])
    
     
    
    
        
    


    sql=f"SELECT SC_WICKET,OVER_STATUS FROM VW_SCORE_WICKET where id={id} and TEAM='{TEAM2}'"
    data=get_data(sql)
    score=data['SC_WICKET'].to_list()[0]+'('+data['OVER_STATUS'].to_list()[0]+')'

    col1,_, col2 = st.columns(3)

    with col1:
        st.write(TEAM2,' Innings')
    with col2:
        st.write(score)    
    
    

    sql=f"select BATTER,case when STATUS is null then 'Not Out' else STATUS END AS STATUS,RUNS::string RUNS,BOWL,\"4s\",\"6s\",SR from CRICKET.RAW.VW_T20_DELIVERIES_SCORECARD_BAT_ORDER where id={id} and TEAM='{TEAM2}' ORDER BY BATTER_ORDER"
    data=get_data(sql)
    st.dataframe(data,hide_index=True)
    sql=f"SELECT EXTRAS FROM VW_T20_EXTRAS where id={id} and TEAM='{TEAM2}'"
    data=get_data(sql)
    col1,_, col2 = st.columns(3)
    with col1:
        st.write('Extras')
    with col2:
        st.write(data['EXTRAS'].to_list()[0])

    sql=f"SELECT BOWLER,OVERS,RUNS::string RUNS,NVL(WD,0)::string  WD,NVL(NB,0)::string  NB,W FROM VW_T20_DELIVERIES_BOWLING_SCORE_ORDER where id={id} and TEAM='{TEAM2}' ORDER BY bowler_order"
    data=get_data(sql)
    st.dataframe(data,hide_index=True)

    sql=f"SELECT PLAYER_LIST FROM VW_PLAYET_YET_BAT where id={id} and TEAM='{TEAM2}'"
    data=get_data(sql)
    if data['PLAYER_LIST'].to_list().__len__()>0:
        st.write('Did not Bat yet')
        st.write(data['PLAYER_LIST'].to_list()[0])

    sql=f"SELECT RESULT FROM VW_T20_FALL_WICKETS where id={id} and TEAM='{TEAM2}'"
    data=get_data(sql)
    if data['RESULT'].to_list().__len__()>0:
        st.write('Fall of Wickets')
        st.write(data['RESULT'].to_list()[0])

      
        

    

