#100종목 전처리
import sqlite3
import pandas as pd

#여기는 각자 수정 필요
directory = "C:/Users/chael/Untitled Folder/vs/SWdata"
directory2 = "C:/Users/chael/Untitled Folder/vs/SWdata/FS/" #재무제표 csv있는 폴더
directory3 = "C:/Users/chael/Untitled Folder/vs/SWdata/final/" #최종 저장할 폴더 미리 생성하고 경로 지정해야함

jongmoc_csv_list=["A005930",
                    "A000660",
                    "A035420",
                    #"A005935",보조지표 2개 없음 VR,VR_SIGNAL
                    #"A051910",보조지표 2개 없음 VR,VR_SIGNAL
                    "A005380",
                    "A207940", #재무 3개
                    #"A035720",보조지표 2개 없음 VR,VR_SIGNAL
                    "A006400",
                    "A068270",
                    "A000270",
                    "A005490",
                    "A012330",
                    "A066570",
                    "A051900",
                    "A028260",
                    "A105560",
                    "A036570",
                    "A017670",
                    "A096770",
                    "A055550",
                    "A034730",
                    "A032830",
                    "A003550",
                    "A090430",
                    "A015760",
                    "A018260",
                    "A009150",
                    "A086790",
                    "A003670",
                    "A033780",
                    "A251270", #재무5개
                    #"A302440",보조 3개뿐
                    "A011170",
                    "A018880",
                    "A011200",
                    "A010950",
                    "A000810",
                    "A009830",
                    "A326030",#재무 18개
                    "A009540",
                    "A034220",
                    "A352820",#재무19개
                    "A010130",
                    "A316140",#재무18개
                    "A086280",
                    "A011780",
                    "A030200",
                    "A024110",
                    "A097950",
                    "A006800",
                    "A004020",
                    "A035250",
                    "A019170",
                    "A161390",
                    "A032640",
                    "A002790",
                    "A271560",#재무
                    "A011070",
                    "A069500",
                    "A139480",
                    "A000720",
                    "A071050",
                    "A003490",
                    "A021240",
                    "A034020",
                    "A006280",
                    "A000100",
                    "A088980",
                    "A267250",#재무
                    "A010140",
                    "A011790",
                    "A008930",
                    "A000120",
                    "A029780",
                    "A180640",
                    "A128940",
                    "A028670",
                    "A005387",
                    "A006360",
                    "A241560",#재무
                    "A078930",
                    "A026960",
                    "A047810",
                    "A003410",
                    "A004990",
                    "A023530",
                    "A008770",
                    "A016360",
                    "A204320",
                    "A039490",
                    "A285130",#재무
                    "A005830",
                    "A336260",#재무
                    "A005940",
                    "A012750",
                    "A012510",
                    "A008560",
                    "A036460",
                    "A007070"]
                        

jaemu_csv_list=["A005930_삼성전자",
                "A000660_SK하이닉스",
                "A035420_NAVER",
                #"A005935_삼성전자우",
                #"A051910_LG화학",
                "A005380_현대차",
                "A207940_삼성바이오로직스",
                #"A035720_카카오",
                "A006400_삼성SDI",
                "A068270_셀트리온",
                "A000270_기아차",
                "A005490_POSCO",
                "A012330_현대모비스",
                "A066570_LG전자",
                "A051900_LG생활건강",
                "A028260_삼성물산",
                "A105560_KB금융",
                "A036570_엔씨소프트",
                "A017670_SK텔레콤",
                "A096770_SK이노베이션",
                "A055550_신한지주",
                "A034730_SK",
                "A032830_삼성생명",
                "A003550_LG",
                "A090430_아모레퍼시픽",
                "A015760_한국전력",
                "A018260_삼성에스디에스",
                "A009150_삼성전기",
                "A086790_하나금융지주",
                "A003670_포스코케미칼",
                "A033780_KT&G",
                "A251270_넷마블",#재무 5개
                #"A302440_SK바이오사이언스",
                "A011170_롯데케미칼",
                "A018880_한온시스템",
                "A011200_HMM",
                "A010950_S-Oil",
                "A000810_삼성화재",
                "A009830_한화솔루션",
                "A326030_SK바이오팜",
                "A009540_한국조선해양",
                "A034220_LG디스플레이",
                "A352820_빅히트",
                "A010130_고려아연",
                "A316140_우리금융지주",
                "A086280_현대글로비스",
                "A011780_금호석유",
                "A030200_KT",
                "A024110_기업은행",
                "A097950_CJ제일제당",
                "A006800_미래에셋대우",
                "A004020_현대제철",
                "A035250_강원랜드",
                "A019170_신풍제약",
                "A161390_한국타이어앤테크놀로지",
                "A032640_LG유플러스",
                "A002790_아모레G",
                "A271560_오리온",
                "A011070_LG이노텍",
                "A069500_KODEX200",
                "A139480_이마트",
                "A000720_현대건설",
                "A071050_한국금융지주",
                "A003490_대한항공",
                "A021240_코웨이",
                "A034020_두산중공업",
                "A006280_녹십자",
                "A000100_유한양행",
                "A088980_맥쿼리인프라",
                "A267250_현대중공업지주",
                "A010140_삼성중공업",
                "A011790_SKC",
                "A008930_한미사이언스",
                "A000120_CJ대한통운",
                "A029780_삼성카드",
                "A180640_한진칼",
                "A128940_한미약품",
                "A028670_팬오션",
                "A005387_현대차2우B",
                "A006360_GS건설",
                "A241560_두산밥캣",
                "A078930_GS",
                "A026960_동서",
                "A047810_한국항공우주",
                "A003410_쌍용양회",
                "A004990_롯데지주",
                "A023530_롯데쇼핑",
                "A008770_호텔신라",
                "A016360_삼성증권",
                "A204320_만도",
                "A039490_키움증권",
                "A285130_SK케미칼",
                "A005830_DB손해보험",
                "A336260_두산퓨얼셀",
                "A005940_NH투자증권",
                "A012750_에스원",
                "A012510_더존비즈온",
                "A008560_메리츠증권",
                "A036460_한국가스공사",
                "A007070_GS리테일"]
                

for n in range(len(jongmoc_csv_list)):
    jongmoc = jongmoc_csv_list[n]
    jongmoc_jaemu = jaemu_csv_list[n]

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
    
    #Profit, Profit2, Volatility 증감 계산
    result5['Profit']=result5['Close']-result5['Open']
    result5['Volatility']=result5['High']-result5['Low']
    result6=result5.copy()
    result6.loc[result6['Profit']>=0,'Profit2']='1'
    #result6.loc[result6['profit']==0,'profit2']='Zero'
    result6.loc[result6['Profit']<0,'Profit2']='0'

    #날짜 형식 정수로 변환    
    result6['key_date']=result6['key_date'].astype(int)

    #엑셀로 저장
    result6.to_csv(directory3+'%s_merge.csv' % jongmoc, header=True, index=False, encoding='CP949')
    print('%s 저장 완료-------------------------------------------'%jongmoc)
    print(n)
