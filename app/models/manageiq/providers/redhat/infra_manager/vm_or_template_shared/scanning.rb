module ManageIQ::Providers::Redhat::InfraManager::VmOrTemplateShared::Scanning
  def perform_metadata_scan(ost)
    require 'MiqVm/MiqRhevmVm'

    attr_accessor :storage_server_ids

    log_pref = "MIQ(#{self.class.name}##{__method__})"
    vm_name  = File.uri_to_local_path(ost.args[0])
    $log.debug "#{log_pref} VM = #{vm_name}"

    args1 = ost.args[1]
    args1['ems']['connect'] = true if args1[:mount].blank?

    begin
      $log.debug "perform_metadata_scan: vm_name = #{vm_name}"
      @vm_cfg_file = vm_name
      connect_to_ems(ost)
      miq_vm = MiqRhevmVm.new(@vm_cfg_file, ost)
      scan_via_miq_vm(miq_vm, ost)
    rescue => err
      $log.error "#{log_pref}: #{err}"
      $log.debug err.backtrace.join("\n")
      raise
    ensure
      miq_vm.unmount if miq_vm
    end
  end

  def perform_metadata_sync(ost)
    sync_stashed_metadata(ost)
  end

  def proxies4job(job = nil)
    _log.debug "Enter (RHEVM)"
    msg = 'Perform SmartState Analysis on this VM'

    # If we do not get passed an model object assume it is a job guid
    if job && !job.kind_of?(ActiveRecord::Base)
      jobid = job
      job = Job.find_by_guid(jobid)
    end
    proxies = []
    begin
      proxies = storage2active_proxies
    rescue => e
      msg = e.message
    end

    _log.debug "# proxies = #{proxies.length}"

    if proxies.empty?
      msg = 'No active SmartProxies found to analyze this VM'
      log_proxies(proxies, all_proxy_list, msg, job) if job
    end

    {:proxies => proxies.flatten, :message => msg}
  end

  def validate_smartstate_analysis
    validate_supported_check("Smartstate Analysis")
  end

  NO_STORAGE_ERROR_MSG = "there is no storage defined for this vm"
  SCAN_DETECTED_NO_PROPERLY_CONFIGURED_STORAGE_SERVERS = "scan detected no properly configured storage servers"
  NO_SMART_PROXY_SERVERS_IN_AFFINITY = "No smart proxy servers defined in afinity, please make sure you have defined them. refer to: <documentation>"

  def miq_server_proxies
    _log.debug "Enter (RHEVM)"
    _log.debug "RedHat: storage_id.blank? = #{storage_id.blank?}"
    raise NO_STORAGE_ERROR_MSG if storage_id.blank?
    srs = self.class.miq_servers_for_scan
    _log.debug "srs.length = #{srs.length}"
    miq_servers = srs.select do |svr|
      svr.vm_scan_storage_affinity? ? storage_server_ids.detect { |id| id == svr.id }
      : storage_server_ids.empty?
    end
    _log.debug "miq_servers1.length = #{miq_servers.length}"
    raise SCAN_DETECTED_NO_PROPERLY_CONFIGURED_STORAGE_SERVERS if miq_servers.empty?
    miq_servers = select_miq_servers(miq_servers)
    _log.debug "miq_servers2.length = #{miq_servers.length}"
    raise NO_SMART_PROXY_SERVERS_IN_AFFINITY if miq_servers.empty?
    miq_servers
  end

  private

  def storage_server_ids
    return @storage_server_ids if @storage_server_ids
    grouped_storage_server_ids = storages.collect { |s| s.vm_scan_affinity.collect(&:id) }.reject(&:blank?)
    _log.debug "grouped_storage_server_ids.length = #{grouped_storage_server_ids.length}"

    @storage_server_ids = grouped_storage_server_ids.flatten
    _log.debug "storage_server_ids.length = #{@storage_server_ids.length}"
    @storage_server_ids
  end

  def select_miq_servers(miq_servers)
    miq_servers.select do |svr|
      smart_proxy_affinity(svr) || evm_has_same_storage_as_vm?(svr.vm)
    end
  end

  def smart_proxy_affinity(svr)
    svr.vm_scan_storage_affinity? && server_started_in_my_zone?(svr)
  end

  def evm_has_same_storage_as_vm?(svr_vm)
    # RedHat VMs must be scanned from an EVM server who's host is attached to the same
    # storage as the VM unless overridden via SmartProxy affinity
    return unless svr_vm && svr_vm.host
    missing_storage_ids = storages.collect(&:id) - svr_vm.host.storages.collect(&:id)
    return missing_storage_ids.empty?
  end

  def svr_started_in_my_zone?(svr)
    svr.status == "started" && svr.has_zone?(my_zone)
  end

  def storage2active_proxies(all_proxy_list = nil)
    _log.debug "Enter (RHEVM)"

    all_proxy_list ||= storage2proxies
    _log.debug "all_proxy_list.length = #{all_proxy_list.length}"
    proxies = all_proxy_list.select(&:is_proxy_active?)
    _log.debug "proxies.length = #{proxies.length}"

    proxies
  end

  # Moved from MIQExtract.rb
  # TODO: Should this be in the ems?
  def connect_to_ems(ost)
    log_header = "MIQ(#{self.class.name}.#{__method__})"

    # Check if we've been told explicitly not to connect to the ems
    # TODO: See vm_scan.rb: config_ems_list() - is this always false for RedHat?
    if ost.scanData.fetch_path("ems", 'connect') == false
      $log.debug "#{log_header}: returning, ems/connect == false"
      return
    end

    st = Time.now
    ems_display_text = "ems(directly):#{ext_management_system.address}"
    $log.info "#{log_header}: Connecting to [#{ems_display_text}] for VM:[#{@vm_cfg_file}]"

    begin
      ost.miqRhevm = ext_management_system.rhevm_inventory
      $log.info "Connection to [#{ems_display_text}] completed for VM:[#{@vm_cfg_file}] in [#{Time.now - st}] seconds"
    rescue Timeout::Error => err
      msg = "#{log_header}: Connection to [#{ems_display_text}] timed out for VM:[#{@vm_cfg_file}] with error [#{err}] after [#{Time.now - st}] seconds"
      $log.error msg
      raise
    rescue Exception => err
      msg = "#{log_header}: Connection to [#{ems_display_text}] failed for VM:[#{@vm_cfg_file}] with error [#{err}] after [#{Time.now - st}] seconds"
      $log.error msg
      raise
    end
  end
end
