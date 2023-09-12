// Load the webcomponentsjs polyfill
require('script!../bower_components/webcomponentsjs/webcomponents.js')

window.Polymer = {
    dom: 'shadow',
    lazyRegister: true,
    useNativeCSSProperties: true,
}

// Require your main webcomponent file (that can be just a file filled with html imports, custom styles or whatever)
require('vulcanize?es6=false&base=./!./imports.html')

// Require our styles
import "./main.css"

window.addEventListener('WebComponentsReady', () => {
    let Elm = require('./Main.elm');
    let root = document.getElementById('root');
    let app = Elm.Main.embed(root, {
        cs_control_port: process.env.CS_CONTROL_PORT,
        cs_port: process.env.CS_PORT,
        cs_host: process.env.CS_HOST,
        cs_proto: process.env.CS_PROTO,
        cs_admin_key: process.env.CS_ADMIN_KEY,
        cs_admin_secret: process.env.CS_ADMIN_SECRET,
        log_level: process.env.LOG_LEVEL,
        log_dir: process.env.LOG_DIR
    });

    let main = require("./js/main.js");
    main.start(app);
});
