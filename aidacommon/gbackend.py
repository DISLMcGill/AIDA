import logging;
import threading;

import weakref;

import uuid;

import dash;
import dash_core_components as dcc;
import dash_html_components as html;
import flask;

from aidacommon.aidaConfig import AConfig;

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

class GBackendApp(threading.Thread):

    _DBC_URL_Mapping_ = weakref.WeakValueDictionary();
    GBackendAppObj = None;
    _dashPort_ = None;
    _app_ = None;

    #def __init__(self):
    #    ;

    def __new__(cls, dashPort):
        if(GBackendApp.GBackendAppObj is not None):
            raise Exception('Singleton property violation attempt on GBackendApp.')
        return super().__new__(cls);

    def __init__(self, dashPort):
        GBackendApp.GBackendAppObj = self;
        GBackendApp._dashPort_ = dashPort;
        super().__init__();

    @property
    def app(self):
        return GBackendApp._app_;

    @classmethod
    def getGBackendAppObj(cls):
        return GBackendApp.GBackendAppObj

    @staticmethod
    def genURLPath(name):
        urlName = str(uuid.uuid4());
        if(name is not None):
            urlName = name + '/' + urlName;
        return '/' + urlName;

    @classmethod
    def addURL(cls, url, dbcObj):
        GBackendApp._DBC_URL_Mapping_[url] = dbcObj;

    @staticmethod
    def wrapGraph(figure):
        return html.Div([dcc.Graph(id=str(uuid.uuid4()).replace('-','X'), figure=figure)])

    def run(self):
        server = flask.Flask('AIDA');
        app = dash.Dash(__name__, external_stylesheets=external_stylesheets, server=server);
        #TODO: Do we need to remove callbacks from removed layouts ?
        app.config.suppress_callback_exceptions = True;
        GBackendApp._app_ = app;

        app.layout = html.Div([
            # represents the URL bar, doesn't render anything
            dcc.Location(id='url', refresh=False),
            #html.H3('This the Dash graphics backend of AIDA server.'),
            # content will be rendered in this element
            html.Div(id='page-content')
        ]);

        @app.callback(dash.dependencies.Output('page-content', 'children'), [dash.dependencies.Input('url', 'pathname')])
        def display_page(pathname):
            try:
                ###For VLDB demo monitoring
                AConfig.AIDALOG.log(['A','P'], 1);
                dbc = GBackendApp._DBC_URL_Mapping_[pathname];
                return dbc.getPlotLayout(pathname)
            except:
                #logging.exception("Error cannot locate URL {} among {}".format(pathname, str([u for u in GBackendApp._DBC_URL_Mapping_.keys()])));
                logging.exception("Error cannot locate URL {}".format(pathname));
                return html.Div([ html.H3('Hello!. This is the Dash graphics backend of AIDA server. Your requested URL was not found {}'.format(pathname)) ])
            finally:
                AConfig.AIDALOG.log(['A','P'], 0);

        AConfig.AIDALOG.setAppObj(app);
        AConfig.AIDALOG.setPlotLayout();
        GBackendApp.addURL('/system/perf', AConfig.AIDALOG);

        #app.run_server(debug=False);
        server.run(host='0.0.0.0', port=GBackendApp._dashPort_);

