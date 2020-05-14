axios.interceptors.request.use((config) => {
    config.headers['X-Requested-With'] = 'XMLHttpRequest';
    let regex = /.*csrftoken=([^;.]*).*$/; // 用于从cookie中匹配 csrftoken值
    config.headers['X-CSRFToken'] = document.cookie.match(regex) === null ? null : document.cookie.match(regex)[1];
    return config
});

const VALID_FUNCTIONS = {
    required(value) {
        return typeof(value) !== 'undefined' && value !== null && value.length > 0
    },
    min_length(value, pattern) {
        return typeof(value) !== 'undefined' && value !== null && value.length >= pattern
    },
    max_lenth(value, pattern) {
        return typeof(value) !== 'undefined' && value !== null && value.length <= pattern
    },
    min_value(value, pattern) {
        return typeof(value) !== 'undefined' && value !== null && value >= pattern
    },
    max_value(value, pattern) {
        return typeof(value) !== 'undefined' && value !== null && value <= pattern
    },
    must_be(value, patterrn) {
        return typeof(value) !== 'undefined' && value !== null && value === pattern
    },
    pattern(value, pattern) {
        return pattern.test(value)
    }
}

var madlee_post = function(url, self, fields, handle) {
    var parms = {}
    for (var j = 0; j < fields.length; ++j) {
        var k = fields[j]
        var val = self[k].value
        if (typeof(val) !== 'undefined') {
            var valid = self[k].valid
            if (typeof(valid) !== 'undefined') {
                for (var i = 0; i < valid.length; ++i) {
                    var vi = valid[i]
                    if (!VALID_FUNCTIONS[valid[i].type](val, vi.pattern)) {
                        self[k].has_error = true
                        self[k].hint = valid[i].hint
                        return false
                    }
                }
            }
            parms[k] = val
        }
        else {
            var x = self[k]
            var t = typeof(x) 
            if (t === 'function') {
                parms[k] = x()
            }
            else {
                parms[k] = self[k]
            }
        }
    }
    axios.post(url, parms).then(function(response) {
        if (response.data.status === 'OK') {
            handle(self, response.data.data)
        }
        else {
            self.message = response.data.message
            var data = response.data.data
            for (var k in data) {
                if (typeof(self[k]) !== 'undefined') {
                    self[k].has_error = true
                    self[k].hint = data[k]
                }
            }
        }
    }).catch(function(error) { 
        if (typeof(error.message) !== 'undefined') {
            self.message = error.message
        }
    })
}


PATTERN_EMAIL =  /^([A-Za-z0-9_\-\.])+\@([A-Za-z0-9_\-\.])+\.([A-Za-z]{2,4})$/;


var file_icon = function(filename) {
    filename = filename.toLowerCase()
    var tokens = filename.split('.')
    var ext = tokens[tokens.length-1]
    if (ext === 'txt' || ext === 'text') {
        return 'fa-file-text-o'
    }
    else if (ext === 'xlsx' || ext === 'xls' || ext === 'csv') {
        return 'fa-file-excel-o'
    }
    else if (ext === 'pdf') {
        return 'fa-file-pdf-o'
    }
    return 'fa-file-o'
}
