import sqlite3
import pandas as pd

#여기 각자 수정해서 쓰기~, 재무재표는 FS폴더안에 모아둠
directory = "C:/Users/chael/Untitled Folder/vs/SWdata"
jongmoc = "A071050"
jongmoc_jaemu = "A071050_한국금융지주"
directory2 = "C:/Users/chael/Untitled Folder/vs/SWdata/FS/"

#30분봉 테이블 가져오기
con = sqlite3.connect(directory+"/kospi100_stock_data_30minute.db")
cursor = con.cursor()
cursor.execute("SELECT * FROM "+jongmoc)
jongmoc_30min_data=cursor.fetchall()
jongmoc_30min_dataframe=pd.DataFrame(jongmoc_30min_data)
jongmoc_30min_dataframe= jongmoc_30min_dataframe.loc[:,[1,2,3,4,5,7,8,9,10]]

#일별 테이블 가져오기
con1 = sqlite3.connect(directory+"/kospi100_stock_data_day.db")
cursor1 = con1.cursor()
cursor1.execute("SELECT * FROM "+jongmoc)
jongmoc_day_data=cursor1.fetchall()
jongmoc_day_dataframe=pd.DataFrame(jongmoc_day_data)
#jongmoc_day_dataframe= jongmoc_day_dataframe.loc[:,-0]

#보조지표 가져오기
con2 = sqlite3.connect(directory+"/kospi100_보조지표.db")
cursor2 = con2.cursor()
cursor2.execute("SELECT * FROM "+jongmoc)
jongmoc_sub_data=cursor2.fetchall()
jongmoc_sub_dataframe=pd.DataFrame(jongmoc_sub_data)

#재무제표 가져오기
"""
con3 = sqlite3.connect(directory+"/kospi100_재무제표.db")
cursor3 = con3.cursor()
cursor3.execute("SELECT * FROM "+jongmoc_jaemu)
jongmoc_jaemu_data=cursor3.fetchall()
jongmoc_jaemu_dataframe=pd.DataFrame(jongmoc_jaemu_data)
"""
jongmoc_jaemu_dataframe = pd.read_csv(directory2+jongmoc_jaemu+'.csv',encoding='CP949')

#외부지표 가져오기
jongmoc_market_data = pd.read_csv(directory+'/international_환율,금,유가.csv',encoding='CP949')

#변수명 입력
col_list1=['Date', 'Open', 'High', 'Low', 'Close',
        '거래량','거래대금','누적체결매수수량','상장주식수']

col_list2=['index','Date', 'Open_D', 'High_D', 'Low_D', 'Close_D',
        '전일대비','거래량_D','거래대금_D','상장주식수_D','시가총액',
        '외인주문한도수량','외인주문가능수량','외인현보유수량','외인현보유비율',
        '수정주가일자','수정주가비율','기관순매수','기관누적순매수','대비부호']

col_list3=['Date','MA_5','MA_20','MA_60','SLOW_K','SLOW_D','MACD','MACD_SIGNAL',
           'MACD_OSCILLATOR','RSI','RSI_SIGNAL','BWMACD','BWM_SIGNAL','BWM_OSCILLATOR',
           'TSF','TSF_SIGNAL','ZigZag1','ZigZag2','Bol_UP','Bol_DOWN','Bol_MID',
           'OBV','OBV_SIGNAL','VR','VR_SIGNAL']

#col_list4 = ['결산년도', '매출액', '영업이익', '당기순익','BPS','PER','PBR','EPS','부채율','유보율','매출증가','영익증가','영익률','유동비율','자기자본','자산증가','매출이익','ROA']

col_list5 = ['결산년도_P', '매출액_P', '영업이익_P', '당기순익_P','BPS_P','PER_P','PBR_P','EPS_P','부채율_P',
            '유보율_P','매출증가_P','영익증가_P','영익률_P','유동비율_P','자기자본_P','자산증가_P','매출이익_P','ROA_P',
             'jaemu_key_date3']

jongmoc_30min_dataframe.columns = col_list1
jongmoc_day_dataframe.columns = col_list2
jongmoc_sub_dataframe.columns = col_list3
#jongmoc_jaemu_dataframe.columns = col_list4

#병합 할 기준이 되는 key_date 만들기
key_date_min=[]
for i in range(len(jongmoc_30min_dataframe)) :
    key_date_min.append(str(jongmoc_30min_dataframe['Date'][i])[:8])

jongmoc_30min_dataframe['key_date'] = key_date_min

key_date_day=[]
for i in range(len(jongmoc_day_dataframe)) :
    key_date_day.append(str(jongmoc_day_dataframe['Date'][i])[:8])

jongmoc_day_dataframe['key_date'] = key_date_day

key_date_sub=[]
for i in range(len(jongmoc_sub_dataframe)) :
    key_date_sub.append(str(jongmoc_sub_dataframe['Date'][i])[:8])

jongmoc_sub_dataframe['key_date'] = key_date_sub

key_date_mak=[]
for i in range(len(jongmoc_market_data)) :
    key_date_mak.append(str(jongmoc_market_data['date'][i])[:4]
    +str(jongmoc_market_data['date'][i])[5:7]
    +str(jongmoc_market_data['date'][i])[8:10])

jongmoc_market_data['key_date'] = key_date_mak

#재무제표 결산 년도 형식을 8자리로 변환(20년09월(3Q)->20200931) 
#jaemu_date_list에 20200931를 str로 저장
jaemu_date_list=[]
for i in range(len(jongmoc_jaemu_dataframe)):
    jaemu_date_list.append('20'+str(jongmoc_jaemu_dataframe['결산년도'][i])[:2]
    +str(jongmoc_jaemu_dataframe['결산년도'][i])[3:5]+'31')
#    jaemu_date_list를 int로 변환
for i in range(len(jaemu_date_list)):
    jaemu_date_list[i]=int(jaemu_date_list[i])


#일별 keydate를 복사, 비교용으로 쓰려고   
day_date_list=[]
for i in range(len(jongmoc_day_dataframe['key_date'])):
    day_date_list.append(int(jongmoc_day_dataframe['key_date'][i]))
#int형,결산년도를 숫자로만 바꾼값
jongmoc_jaemu_dataframe['jaemu_key_date2'] = jaemu_date_list

last_jaemu_date=jaemu_date_list[0]

if(last_jaemu_date%10000<1231):
    plus300=last_jaemu_date+300 #3달을 더해줌
elif(last_jaemu_date%10000==1231):
    plus300=last_jaemu_date+9100 #년도 바꾸고, 3월로 바꿔줌

jaemu_date_list.insert(0,plus300)   


if(plus300%10000<1231):
    plus600=plus300+300 #3달을 더해줌
elif(plus300%10000==1231):
    plus600=plus300+9100 #년도 바꾸고, 3월로 바꿔줌

jaemu_date_list.insert(0,plus600)   

if(plus600%10000<1231):
    plus900=plus600+300 #3달을 더해줌
elif(plus300%10000==1231):
    plus900=plus600+9100 #년도 바꾸고, 3월로 바꿔줌

jaemu_date_list.insert(0,plus900)
#jongmoc_day_dataframe['jaemu_key_date_now'] = jaemu_key_date_now

#일별데이터에 재무제표와 합병할 수 있도록 직전 분기 jaemu_key_date_now를 생성
jaemu_key_date=[]

for i in range(len(day_date_list)):
    for k in range(len(jaemu_date_list)-1):
        if((day_date_list[i]<=jaemu_date_list[k])&(day_date_list[i]>jaemu_date_list[k+1])):
            a=jaemu_date_list[k]
            break
        else:
            a=0
    jaemu_key_date.append(a)

jongmoc_day_dataframe['jaemu_key_date1'] = jaemu_key_date

#직전 분기 재무제표 병합용 jaemu_key_date2 생성
jaemu_key_date2=[]

for i in range(len(jongmoc_day_dataframe['jaemu_key_date1'])):
    raw=jaemu_key_date[i]
    
    #20160331같은 raw를 331형식으로 바꿔서 직전 분기 도출
    if(raw%10000>331):
        a=raw-300 #3달을 빼 줌
    elif(raw%10000==331):
        a=raw-9100 #년도 바꾸고, 12월로 바꿔줌
    else:
        a=0
        
    jaemu_key_date2.append(a)
    
jongmoc_day_dataframe['jaemu_key_date2'] = jaemu_key_date2


#직전의 직전 분기 재무제표 병합용 jaemu_key_date3 생성
jaemu_key_date3=[]

for i in range(len(jongmoc_day_dataframe['jaemu_key_date2'])):
    raw=jaemu_key_date2[i]
    
    #20160331같은 raw를 331형식으로 바꿔서 직전 분기 도출
    if(raw%10000>331):
        a=raw-300 #3달을 빼 줌
    elif(raw%10000==331):
        a=raw-9100 #년도 바꾸고, 12월로 바꿔줌
    else:
        a=0
        
    jaemu_key_date3.append(a)
    
jongmoc_day_dataframe['jaemu_key_date3'] = jaemu_key_date3




jongmoc_jaemu_dataframe_past=jongmoc_jaemu_dataframe.copy() #과거분기용 재무데이터프레임 따로 생성

jongmoc_jaemu_dataframe_past.columns = col_list5 #변수명_P로 변경

    
#데이터 형식 변환 (9,999 -> 9999), float 타입으로 변환       
for i in range(1,len(jongmoc_jaemu_dataframe.columns)-1):#결산년도, jaemu_key_date는 빼주기위한 범위
    try:
        jongmoc_jaemu_dataframe[jongmoc_jaemu_dataframe.columns[i]]=jongmoc_jaemu_dataframe[jongmoc_jaemu_dataframe.columns[i]].str.replace(",","")
    except AttributeError as e:
        print(jongmoc_jaemu_dataframe.columns[i]+" 형식 변환 에러")
        
    jongmoc_jaemu_dataframe[jongmoc_jaemu_dataframe.columns[i]]=jongmoc_jaemu_dataframe[jongmoc_jaemu_dataframe.columns[i]].astype(float)


print("--------------------------")
for i in range(1,len(jongmoc_jaemu_dataframe_past.columns)-1):
    try:
        jongmoc_jaemu_dataframe_past[jongmoc_jaemu_dataframe_past.columns[i]]=jongmoc_jaemu_dataframe_past[jongmoc_jaemu_dataframe_past.columns[i]].str.replace(",","")
    except AttributeError as e:
        print(jongmoc_jaemu_dataframe_past.columns[i]+" 형식 변환 에러")
    
    jongmoc_jaemu_dataframe_past[jongmoc_jaemu_dataframe_past.columns[i]]=jongmoc_jaemu_dataframe_past[jongmoc_jaemu_dataframe_past.columns[i]].astype(float)
    
    
print("--------------------------")        
for i in range(1,len(jongmoc_market_data.columns)-1):
    try:
        jongmoc_market_data[jongmoc_market_data.columns[i]]=jongmoc_market_data[jongmoc_market_data.columns[i]].str.replace(",","")
    except AttributeError as e: 
        print(jongmoc_market_data.columns[i]+" 형식 변환 에러")      

    jongmoc_market_data[jongmoc_market_data.columns[i]]=jongmoc_market_data[jongmoc_market_data.columns[i]].astype(float)                                                                                                           


print("--------------------------")

#필요없어진 변수들 삭제
del jongmoc_market_data['date']
del jongmoc_day_dataframe['index']
del jongmoc_day_dataframe['Date']
del jongmoc_sub_dataframe['Date']
del jongmoc_jaemu_dataframe['결산년도']
del jongmoc_jaemu_dataframe_past['결산년도_P']

#key_date에 맞게 병합하고 엑셀로 저장
result = pd.merge(jongmoc_30min_dataframe, jongmoc_day_dataframe, on='key_date',how="left")
result2 = pd.merge(result, jongmoc_sub_dataframe, on='key_date',how="left")
result3 = pd.merge(result2, jongmoc_market_data, on='key_date',how="left")
result4 = pd.merge(result3, jongmoc_jaemu_dataframe, on = 'jaemu_key_date2', how = "left")
result5 = pd.merge(result4, jongmoc_jaemu_dataframe_past, on = 'jaemu_key_date3', how = "left")
del result5['jaemu_key_date1']
del result5['jaemu_key_date2']
del result5['jaemu_key_date3']

#재무제표 증감 계산
for i in range(1,len(jongmoc_jaemu_dataframe.columns)-1):
    result5[jongmoc_jaemu_dataframe.columns[i]+'_증감']=result5[jongmoc_jaemu_dataframe.columns[i]]-result5[jongmoc_jaemu_dataframe.columns[i]+'_P']

#날짜 형식 정수로 변환    
result5['key_date']=result5['key_date'].astype(int)

#엑셀로 저장
result5.to_csv('%s_merge_real.csv' % jongmoc, header=True, index=False, encoding='CP949')