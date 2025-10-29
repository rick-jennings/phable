# escape=`
# syntax=docker/dockerfile:1

ARG JDK_VERSION=17

FROM eclipse-temurin:$JDK_VERSION AS base

WORKDIR /app/

ARG FAN_REL_VER=1.0.82

# Build Fantom from source
RUN <<EOF
  apt-get -q update
  apt-get -q install -y git curl unzip
  curl -fsSL https://github.com/fantom-lang/fantom/releases/download/v${FAN_REL_VER}/fantom-${FAN_REL_VER}.zip -o fantom.zip
  unzip fantom.zip
  mv fantom-${FAN_REL_VER} rel
  chmod +x rel/bin/*
  chmod +x rel/adm/*
EOF

#Get latest fantom release
RUN git clone -b master --depth 1 --single-branch https://github.com/fantom-lang/fantom fan `
  && rm -rf fan/.git*

RUN <<EOF
  echo "\n\njdkHome=${JAVA_HOME}/\ndevHome=/app/fan/\n" >> rel/etc/build/config.props
  echo "\n\njdkHome=${JAVA_HOME}/" >> fan/etc/build/config.props
  rel/bin/fan fan/src/buildall.fan superclean
  rel/bin/fan fan/src/buildboot.fan compile
  fan/bin/fan fan/src/buildpods.fan compile
EOF

ENV FAN_SUBSTITUTE=/app/rel/
ENV PATH=$PATH:/app/fan/bin
#FANTOM SOURCE END


#XETO START
# ADD git@github.com:Project-Haystack/xeto.git xeto
RUN git clone -b master --depth 1 --single-branch https://github.com/Project-Haystack/xeto xeto `
  && rm -rf xeto/.git*
#XETO END


#HAXALL START
WORKDIR /app/
RUN git clone -b main --depth 1 --single-branch https://github.com/haxall/haxall haxall `
  && rm -rf haxall/.git*
WORKDIR /app/haxall/
RUN <<EOF
  chmod +x bin/*
  echo "path=../xeto;../phable-test" > fan.props
  fan src/build.fan clean compile
EOF
#HAXALL END
