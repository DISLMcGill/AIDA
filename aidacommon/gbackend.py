import threading;

import weakref;

import uuid;

import dash;
import flask;

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

        # app.layout = html.Div(children=[
        #     #html.H1(children='Hello Dash'),
        #
        #     #html.Div(children=''' Dash: A web application framework for Python. '''),
        #
        #     dcc.Graph(
        #         id='example-graph',
        #         figure={
        #             'data': [
        #                 {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
        #                 {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
        #             ],
        #             'layout': {
        #                 'title': 'Dash Data Visualization'
        #             }
        #         }
        #     )
        # ]);

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
                dbc = GBackendApp._DBC_URL_Mapping_[pathname];
                return dbc.getPlotLayout(pathname)
            except:
                pass;
            return html.Div([ html.H3('Hello!. This is the Dash graphics backend of AIDA server. Your requested URL was not found {}'.format(pathname)) ])

        #app.run_server(debug=False);
        server.run(host='0.0.0.0', port=GBackendApp._dashPort_);

