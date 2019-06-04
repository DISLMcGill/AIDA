###For VLDB demo monitoring

import time;
import datetime;
##import pymonetdb;
import logging;


import sqlite3;
import pandas as pd;
from threading import Lock;

class AIDALog:

    #__con__ = sqlite3.connect('/tmp/alogdb', check_same_thread=False);
    __con__ = sqlite3.connect(':memory:', check_same_thread=False);
    __con__.execute("CREATE TABLE IF NOT EXISTS perflog ( ts FLOAT, sy CHAR(1), st TINYINT)");
    __cur__ = __con__.cursor()
    __lock__ = Lock();

    __states__ = {};
    MAXPERFRETENTION = 300;

    __appObj__ = None

##    __con__ = pymonetdb.Connection('aidalog',username='aidalog',password='aidalog',autocommit=True);
##    __cur__ = __con__.cursor();
    #Called by bootstrap method to set the binding dash app obj.
    @classmethod
    def setAppObj(cls, appobj):
        cls.__appObj__ = appobj;

    @classmethod
    def stateChanged(cls, sy, st):
        try:
            prev = cls.__states__[sy];
            if(st > 0):
                cls.__states__[sy] += 1;
            else:
                cls.__states__[sy] -= 1;
                if(cls.__states__[sy] < 0):
                    cls.__states__[sy] = 0;

            if(prev>0 and st==1 or prev==0 and st==0):
                return False;
        except KeyError:
            pass;

        cls.__states__[sy] = st;
        return True;


    @classmethod
    def log(cls, sys, st):
        try:
            ##cls.__cur__.execute("INSERT INTO aidalog.perflog(ts, sy, st) VALUES({}, '{}', {})".format(time.time(), sy, st));
            cls.__lock__.acquire();
            ts = time.time();
            if(isinstance(sys, str)):
                sys = [sys];
            for sy in sys:
                if(cls.stateChanged(sy,st)):
                    #logging.info("INSERT INTO perflog(ts, sy, st) VALUES({}, '{}', {})".format(ts, sy, st));
                    #cls.__cur__.execute("INSERT INTO perflog(ts, sy, st) VALUES({}, '{}', {})".format(time.time(), sy, st));
                    cls.__cur__.execute("INSERT INTO perflog(ts, sy, st) VALUES({}, '{}', {})".format(ts, sy, st));
                    ##cls.__cur__.execute("SELECT * FROM perflog WHERE ts = ?", (ts,));
                    ##r = cls.__cur__.fetchone();
                    ##logging.info(r);
        except:
            logging.exception("Error while recording perflog : {} {}".format(sy, st));
        finally:
            cls.__lock__.release();

    @classmethod
    def get_new_data(cls, ts=0):
        ct = time.time();
        tsminstart = ct - cls.MAXPERFRETENTION;
        if ts==0:
            tsstart = tsminstart
        else:
            tsstart = ts+0.000001; #Some weird reason I need to add this float.
        data = None;
        #logging.info('fetching perf data after {}'.format(datetime.datetime.fromtimestamp(tsstart)))

        try:
            cls.__lock__.acquire();
            #data = cls.__cur__.execute("SELECT ts, sy, st FROM perflog WHERE ts >= ?", (tsstart,));
            data = pd.read_sql_query("SELECT ts, sy, st FROM perflog WHERE ts > {} ORDER BY ts;".format(max([tsstart, tsminstart])), cls.__con__);
            #logging.info('Found {} new perf records'.format(len(data)));
            #logging.info('New perf records {}'.format(data));
            tsstart = ct;
        finally:
            cls.__lock__.release();

        if(data is None):
            return None;

        upddts = []; upddsy = []; upddi = [];

        dPlotly = data[data['sy']=='P'];
        if(len(dPlotly)>0):
            #upddts.append(list(dPlotly['ts']))
            upddts.append(list(dPlotly['ts'].apply(lambda x:datetime.datetime.fromtimestamp(x))))
            upddsy.append(list(dPlotly['st']))
        else:
            upddts.append([datetime.datetime.fromtimestamp(tsstart)]);
            upddsy.append([0]);
        upddi.append(0);

        dDB = data[data['sy']=='D'];
        if(len(dDB)>0):
            #upddts.append(list(dDB['ts']))
            upddts.append(list(dDB['ts'].apply(lambda x:datetime.datetime.fromtimestamp(x))))
            upddsy.append(list(dDB['st']))
        else:
            upddts.append([datetime.datetime.fromtimestamp(tsstart)]);
            upddsy.append([0]);
        upddi.append(1);

        dAIDA = data[data['sy']=='A'];
        if(len(dAIDA)>0):
            #upddts.append(list(dAIDA['ts']))
            upddts.append(list(dAIDA['ts'].apply(lambda x:datetime.datetime.fromtimestamp(x))))
            upddsy.append(list(dAIDA['st']))
        else:
            upddts.append([datetime.datetime.fromtimestamp(tsstart)]);
            upddsy.append([0]);
        upddi.append(2);

        #logging.info(('new data', upddts, upddsy, upddi));
        return (upddts, upddsy, upddi)


    @classmethod
    def getPlotLayout(cls, pathname):
        return cls.__layout__;

    @classmethod
    def setPlotLayout(cls):
        from plotly import tools;
        import plotly.graph_objs as go;
        import dash_core_components as dcc;
        import dash_html_components as html;
        from dash.dependencies import Input, Output, State;

        fig = tools.make_subplots(rows=3, cols=1, shared_xaxes=True);
        tracePlotly = go.Scatter(x=[], y=[], name='Plotly', mode='lines', line=dict(shape='hv'), fill='tozeroy')
        traceDB     = go.Scatter(x=[], y=[], name='DB', mode='lines', line=dict(shape='hv'), fill='tozeroy')
        traceAIDA   = go.Scatter(x=[], y=[], name='AIDA', mode='lines', line=dict(shape='hv'), fill='tozeroy')
        fig.add_trace(tracePlotly, 1, 1);
        fig.add_trace(traceDB, 2, 1);
        fig.add_trace(traceAIDA, 3, 1);
        fig['layout']['yaxis'] ={'ticks':'', 'showticklabels':False, 'anchor':'free', 'domain':[0.67,1.0]};
        fig['layout']['yaxis2']={'ticks':'', 'showticklabels':False, 'anchor':'free', 'domain':[0.34,0.66]};
        fig['layout']['yaxis3']={'ticks':'', 'showticklabels':False, 'anchor':'free', 'domain':[0,0.33]};

        #TODO: ids will have to be generated to be unique ?
        layout = html.Div([
            html.H4('System Activity'),
            dcc.Graph(id='system-perf', figure=fig, animate=True),
            dcc.Interval(id='interval-component', interval=2*1000, n_intervals=0),
            html.Div(id='prev-ts', style={'display':'none'})
        ]);

        @cls.__appObj__.callback(Output('system-perf', 'extendData'), [Input('interval-component', 'n_intervals')], [State('prev-ts', 'children')])
        def update_perf_graph(n_intervals, prevts):
            #logging.info((n_intervals, prevts, figure))
            pts = 0;
            if not (prevts is None or n_intervals == 0 or len(prevts)==0):
                #pts = datetime.datetime.strptime(prevts,'%Y-%m-%d %H:%M:%S.%f');
                pts = float(prevts);
            data = cls.get_new_data(pts)
            if(data is None):
                return None;
            return [dict(x=data[0], y=data[1]), data[2]];

        @cls.__appObj__.callback(Output('prev-ts', 'children'), [Input('system-perf', 'extendData')])
        def update_last_ts(extendData):
            try:
                #lastts = max(tslist[-1] for tslist in extendData[0]['x'])
                #logging.info(('last timestamp seen.', type(lastts), lastts))
                lastts = datetime.datetime.strptime(max(tslist[-1] for tslist in extendData[0]['x']), '%Y-%m-%d %H:%M:%S.%f').timestamp();
            except ValueError:
                lastts=0;
            #logging.info((extendData))
            #TODO: parse the max timestamp from extendData and store it here.
            return str(lastts);

        cls.__layout__ = layout;

