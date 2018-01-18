export HARVARD_PRODUCTION_TOPDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
echo "Setting up production tools from ${HARVARD_PRODUCTION_TOPDIR}..."
export PYTHONPATH=${HARVARD_PRODUCTION_TOPDIR}/python:$PYTHONPATH
export PATH=${HARVARD_PRODUCTION_TOPDIR}/bin:$PATH

echo "To submit jobs, use the syntax "
echo "  production.py [config.yml] [-s stage] --[clean | submit | check | status]"