#!/usr/bin/env bash
# Publishes the Spark 3 (spark_2.12 / spark_2.13) and Spark 4 (dataflint-spark4_2.13) plugin
# jars to Maven Central under io.github.wangguangxin.
#
# One-time setup:
#   1. Sign in at https://central.sonatype.com with the WangGuangxin GitHub account. The
#      io.github.wangguangxin namespace is verified automatically.
#   2. Create a user token there (Account > Generate User Token).
#   3. Create a GPG key and upload it: gpg --full-generate-key, then
#      gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
#
# Each release:
#   export SONATYPE_USERNAME=<token user> SONATYPE_PASSWORD=<token password> PGP_PASSPHRASE=<passphrase>
#   ./publish-central.sh            # uploads and releases; the version comes from versionNum in build.sbt
#   ./publish-central.sh --no-release   # uploads only; review and publish in the Central Portal UI
set -euo pipefail

export SONATYPE_USERNAME=wangguangxin SONATYPE_PASSWORD=EWaPTdKo8r8Le4YQYt5T3qUesILvDetC3
export PGP_PASSPHRASE=890212

cd "$(dirname "$0")"

: "${SONATYPE_USERNAME:?set SONATYPE_USERNAME (Central Portal user token)}"
: "${SONATYPE_PASSWORD:?set SONATYPE_PASSWORD (Central Portal user token)}"
: "${PGP_PASSPHRASE:?set PGP_PASSPHRASE}"

release_cmd="sonatypeCentralRelease"
if [[ "${1:-}" == "--no-release" ]]; then release_cmd="sonatypeCentralUpload"; fi

version=$(grep -oE 'lazy val versionNum: String = "[^"]+"' build.sbt | cut -d'"' -f2)
tag="v${version}"
if [[ -n "$(git status --porcelain -- . ../spark-ui/src)" ]]; then
  echo "Working tree has uncommitted changes; commit them first." >&2
  exit 1
fi
# build.sbt uses the release version (no -SNAPSHOT) only when HEAD carries a v* tag.
if ! git tag --points-at HEAD | grep -qx "$tag"; then
  git tag "$tag"
  echo "Created local tag $tag; push it with: git push origin $tag"
fi

JAVA_HOME=${JAVA_HOME:-/home/nvidia/benchmark/runtime/jdk17}
SBT_LAUNCHER=${SBT_LAUNCHER:-$HOME/.sbt/launcher/sbt-launch.jar}
SBT_REPOS=${SBT_REPOS:-$HOME/.sbt/launcher/sbt-repos}

(cd ../spark-ui && npm run deploy)

"$JAVA_HOME/bin/java" -Xmx6g -Xss4m \
  -Dsbt.repository.config="$SBT_REPOS" -Dsbt.override.build.repos=true -Dsbt.supershell=false \
  -jar "$SBT_LAUNCHER" \
  "set ThisBuild / credentials += Credentials(\"Sonatype Nexus Repository Manager\", \"central.sonatype.com\", sys.env(\"SONATYPE_USERNAME\"), sys.env(\"SONATYPE_PASSWORD\"))" \
  "+publishSigned" "$release_cmd" < /dev/null
