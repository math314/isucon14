#!/bin/bash -xe


FILENAME="$1"
cat ${FILENAME} | alp ltsv -m'/api/app/rides/(.+)/evaluation,/api/chair/rides/(.+)/status' --filters "not(Uri matches '^/(assets|images|favicon)')"

