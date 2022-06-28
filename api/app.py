# coding=utf-8
from crypt import methods
from types import MethodType
from flask import Flask,request,render_template,redirect,url_for
from interface import Select, Select_hyper ,query_gene,Insert,query_gene_hyper
import json
from flask_cors import *
app = Flask(__name__)

@app.route('/')
def hello():
   return render_template('hello.html')

@app.route('/searchapi',methods=['POST','GET'])
def Search_api():
   database_path = '../database/'
   headEntity = request.form['headEntity']
   relation = request.form['relation']
   tailEntity  = request.form['tailEntity']
   # headEntity = 'hid120951'
   # relation = 'rdfo'
   # tailEntity = ''
   querystr = ''
   if len(relation) == 0 :
      print('请输入谓词！')
   else :
      if len(headEntity) != 0:
         querystr = query_gene(headEntity,relation,'')
      else:
         if len(tailEntity) != 0:
            querystr = query_gene('',relation,tailEntity)
   print(querystr)
   graphstr_b = Select(database_path,querystr)
   if graphstr_b==None:
      return render_template('hello.html',my_dict="没有结果")
   graphstr = bytes.decode(graphstr_b)
   
   allstrs = graphstr.split('-?-')
   edge=[]
   edges = []
   for i in range(len(allstrs)):
      if i%4==0:
         edge.append(allstrs[i])
      elif i%4==1:
         edge.append(allstrs[i])
      elif i%4==2:
         edge.append(allstrs[i])
      elif i%4==3:
         edge.append(allstrs[i])
         edges.append(edge)
         edge=[]

   data_list = []

   for i in edges:
      ele = {'s':i[0],'p':i[1],'o':i[2],'t':i[3]}
      data_list.append(ele)
      print(ele)
   json_data = json.dumps(data_list,ensure_ascii=False)
   return render_template('hello.html',my_dict=edges)

@app.route('/search_hyper_api',methods=['POST','GET'])
def Search_hyper_api():
   database_path = '../database/'
   subject = request.form['subject']
   predicate = request.form['predicate']
   object  = request.form['object']
   # headEntity = 'hid120951'
   # relation = 'rdfo'
   # tailEntity = ''
   querystr = ''
   if len(subject) != 0 :
      if len(predicate) != 0 :
         if len(object) != 0 :
            querystr = query_gene_hyper(subject,predicate,object)
         elif len(object) == 0 :
            querystr = query_gene_hyper(subject,predicate,'')
      elif len(predicate) == 0 :
         if len(object) != 0 :
            querystr = query_gene_hyper(subject,'',object)
         elif len(object) == 0 :
            querystr = query_gene_hyper(subject,'','')
   elif len(subject) == 0 :
      if len(predicate) != 0 :
         if len(object) != 0 :
            querystr = query_gene_hyper('',predicate,object)
         elif len(object) == 0 :
            querystr = query_gene_hyper('',predicate,'')
      elif len(predicate) == 0 :
         if len(object) != 0 :
            querystr = query_gene_hyper('','',object)
         elif len(object) == 0 :
            querystr = ''
            print('error:请输入参数')

   print(querystr)
   graphstr_b = Select_hyper(database_path,querystr)
   if graphstr_b==None:
      render_template('hello.html',my_dict="没有结果")
   graphstr = bytes.decode(graphstr_b)
   allstrs = graphstr.split('-?-')
   ele = {'hid':'','subject':'','predicate':'','object':[],'stime':'','etime':''}
   data_all = []
   ele['hid'] = allstrs[0]
   for i in range(len(allstrs)):
      if i%6==0:
         if ele['hid']!=allstrs[i]:
            data_all.append(ele)
            ele = {'hid':'','subject':'','predicate':'','object':[],'stime':'','etime':''}
            ele['hid']=allstrs[i]
      elif i%6==1:
         ele['subject']=allstrs[i]
      elif i%6==2:
         ele['predicate'] = allstrs[i]
      elif i%6==3:
         ele['object'].append(allstrs[i])
      elif i%6==4:
         ele['stime'] = allstrs[i]
      elif i%6==5:
         ele['etime'] = allstrs[i]

   for i in data_all:
      print(i)
   # for i in range(len(allstrs)):
   #    if i%6!=5:
   #       print(allstrs[i],end=" ")
   #    else:
   #       print(allstrs[i])
   # edge=[]
   # edges = []
   # for i in range(len(allstrs)):
   #    if i%4==0:
   #       edge.append(allstrs[i])
   #    elif i%4==1:
   #       edge.append(allstrs[i])
   #    elif i%4==2:
   #       edge.append(allstrs[i])
   #    elif i%4==3:
   #       edge.append(allstrs[i])
   #       edges.append(edge)
   #       edge=[]

   # data_list = []

   # for i in edges:
   #    ele = {'s':i[0],'p':i[1],'o':i[2],'t':i[3]}
   #    data_list.append(ele)
   #    print(ele)
   # json_data = json.dumps(data_list,ensure_ascii=False)
   return render_template('hello.html',my_dict=data_all)


@app.route('/insertapi',methods=['POST','GET'])
def Insert_api():
   # print(request.files)
   database_path = '../database/'
   insert_filename = './insertfile'
   insert_file = request.files['file']
   insert_file.save(insert_filename)
   res = Insert(database_path,insert_filename)

   return redirect(url_for('hello'))
if __name__ == '__main__':
   app.run(port=5885,debug=True,host='0.0.0.0')