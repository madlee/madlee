axios.interceptors.request.use((config) => {
    config.headers['X-Requested-With'] = 'XMLHttpRequest';
    let regex = /.*csrftoken=([^;.]*).*$/; // 用于从cookie中匹配 csrftoken值
    config.headers['X-CSRFToken'] = document.cookie.match(regex) === null ? null : document.cookie.match(regex)[1];
    return config
});


var madlee_post = function(url, parms) {
    for (var k in parms) {
        var val = parms[k].value
        var valid = parms[k].valid
        if (typeof(valid) !== 'undefined') {
            if (valid.required) {
                
            }
        }
    }

}