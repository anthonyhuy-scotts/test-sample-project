select distinct ics.firmware_version
from devices_iotdevice di
join irrigation_controller ic on ic.iot_device_id=di.id
join irrigation_controllerstate ics on ic.id = ics.controller_id
where not ics.is_offline
and di.is_active
order by ics.firmware_version