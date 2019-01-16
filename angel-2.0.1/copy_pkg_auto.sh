#!/usr/bin/env bash

# parameters
angelVersion="2.0.1"
Purpose="ps"

Suffix="zip"

LocalHostname="sean"
LocalHostip="128.198.180.112"
AbsolutePath="/opt/modules"
AbsolutePath_Swang="/home/swang/opt/modules"

Command=""
Command_Swang=""

# Figure out where the angel framework is installed
ANGEL_HOME="$(cd "`dirname "$0"`"; pwd)"

# Command
DISTDIR="${AbsolutePath}/angel-${angelVersion}-${Purpose}/${Suffix}"
DISTDIR_Swang="${AbsolutePath_Swang}/angel-${angelVersion}-${Purpose}/${Suffix}"

TARGET="$ANGEL_HOME/dist/target/angel-${angelVersion}-bin.${Suffix}"

Command="scp ${TARGET} ${LocalHostname}@${LocalHostip}:${DISTDIR}"
Command_Swang="scp ${TARGET} ${LocalHostname}@${LocalHostip}:${DISTDIR_Swang}"

echo ${Command}
${Command}
#echo ${Command_Swang}
# ${Command_Swang}
