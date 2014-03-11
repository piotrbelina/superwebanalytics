name "cloudera"
description "Install Oracle Java on Ubuntu and Cloudera"

default_attributes(
  :java => {
     :oracle => {
       "accept_oracle_download_terms" => true
     },
     "remove_deprecated_packages" => false
   }
)

override_attributes(
  "java" => {
    "install_flavor" => "oracle"
  },
  "cloudera" => {
    "installyarn" => false
  }
)


# todo - replace basic by aoe common recipe
run_list(
    "recipe[java]",
    "recipe[cloudera]",
    "recipe[cloudera::flume]",
    "recipe[cloudera::pig]",
    "recipe[cloudera::hbase]"
)
