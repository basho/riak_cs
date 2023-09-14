require("material-components-web-elm/dist/material-components-web-elm.js");
require("material-components-web-elm/dist/material-components-web-elm.css");

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
        // 1. when sent by riak_cs_wm_contol, "process.env.CS_THING"
        //    will be substituted by actual values.  I just don't want
        //    to break the syntax highlightnig in emacs with more
        //    meaningful and clear placeholders like {{CS_THING}}.
        // 2. This is a shim pending development; eventually, creds
        //    will be collected from the user.
        cs_url: process.env.CS_URL,
        cs_admin_key: process.env.CS_ADMIN_KEY,
        cs_admin_secret: process.env.CS_ADMIN_SECRET
    });

    let main = require("./js/main.js");
    main.start(app);
});
