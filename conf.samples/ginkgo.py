GINKGO_FOLDER = 'Ginkgo'

from madlee.misc.lua import LUA_TS_TO_TIME
from madlee.ginkgo.lua_script import get_scripts


LUA_BRANCH_SIZE = '''
local branch_size = function(branch) 
    return 16
end
'''

LUA_BRANCH_SLOT = '''
local branch_slot = function(branch) 
    return 300
end
'''

MADLEE_LUA_FUNCTIONS = {
    'FUNC_TS_TO_SLOT':  LUA_TS_TO_TIME,
    'FUNC_BRANCH_SIZE': LUA_BRANCH_SIZE,
    'FUNC_BRANCH_SLOT': LUA_BRANCH_SLOT
}

DEFALUT_GINKGO_SCRIPTS = get_scripts(
    MADLEE_LUA_FUNCTIONS,
    3600
)

GINGKO_SCRIPTS = {
    'TEST': DEFALUT_GINKGO_SCRIPTS
}

