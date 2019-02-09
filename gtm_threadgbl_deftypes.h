# define ggo_grabbing_crit 0
# define ggt_grabbing_crit gd_region *
# define ggo_blkmod_fail_level 8
# define ggt_blkmod_fail_level int4
# define ggo_blkmod_fail_type 12
# define ggt_blkmod_fail_type int4
# define ggo_block_level 16
# define ggt_block_level int4
# define ggo_boolchain 24
# define ggt_boolchain triple
# define ggo_boolchain_ptr 160
# define ggt_boolchain_ptr triple *
# define ggo_bool_targ_anchor 168
# define ggt_bool_targ_anchor tbp
# define ggo_bool_targ_ptr 192
# define ggt_bool_targ_ptr tbp *
# define ggo_code_generated 200
# define ggt_code_generated boolean_t
# define ggo_codegen_padlen 204
# define ggt_codegen_padlen int4
# define ggo_compile_time 208
# define ggt_compile_time boolean_t
# define ggo_curtchain 216
# define ggt_curtchain triple *
# define ggo_director_ident 224
# define ggt_director_ident mstr
# define ggo_director_mval 240
# define ggt_director_mval mval
# define ggo_director_token 272
# define ggt_director_token char
# define ggo_dollar_zcstatus 276
# define ggt_dollar_zcstatus int4
# define ggo_expr_depth 280
# define ggt_expr_depth unsigned int
# define ggo_expr_start 288
# define ggt_expr_start triple *
# define ggo_expr_start_orig 296
# define ggt_expr_start_orig triple *
# define ggo_defined_symbols 304
# define ggt_defined_symbols struct sym_table *
# define ggo_for_stack_ptr 312
# define ggt_for_stack_ptr oprtype **
# define ggo_ydb_fullbool 320
# define ggt_ydb_fullbool unsigned int
# define ggo_ind_result 328
# define ggt_ind_result mval *
# define ggo_ind_source 336
# define ggt_ind_source mval *
# define ggo_indirection_mval 344
# define ggt_indirection_mval mval
# define ggo_last_source_column 376
# define ggt_last_source_column int
# define ggo_lexical_ptr 384
# define ggt_lexical_ptr char *
# define ggo_linkage_first 392
# define ggt_linkage_first struct linkage_entry *
# define ggo_linkage_last 400
# define ggt_linkage_last struct linkage_entry *
# define ggo_objhash_state 408
# define ggt_objhash_state hash128_state_t
# define ggo_pos_in_chain 448
# define ggt_pos_in_chain triple
# define ggo_rts_error_in_parse 584
# define ggt_rts_error_in_parse boolean_t
# define ggo_s2n_intlit 588
# define ggt_s2n_intlit boolean_t
# define ggo_routine_source_offset 592
# define ggt_routine_source_offset uint4
# define ggo_saw_side_effect 596
# define ggt_saw_side_effect boolean_t
# define ggo_shift_side_effects 600
# define ggt_shift_side_effects int
# define ggo_side_effect_base 608
# define ggt_side_effect_base boolean_t *
# define ggo_side_effect_depth 616
# define ggt_side_effect_depth uint4
# define ggo_side_effect_handling 620
# define ggt_side_effect_handling int
# define ggo_source_buffer 624
# define ggt_source_buffer mstr
# define ggo_source_error_found 640
# define ggt_source_error_found int4
# define ggo_temp_subs 644
# define ggt_temp_subs boolean_t
# define ggo_trigger_compile_and_link 648
# define ggt_trigger_compile_and_link boolean_t
# define ggo_window_ident 656
# define ggt_window_ident mstr
# define ggo_window_mval 672
# define ggt_window_mval mval
# define ggo_window_token 704
# define ggt_window_token char
# define ggo_xecute_literal_parse 708
# define ggt_xecute_literal_parse boolean_t
# define ggo_dbinit_max_delta_secs 712
# define ggt_dbinit_max_delta_secs uint4
# define ggo_dollar_zmaxtptime 716
# define ggt_dollar_zmaxtptime int4
# define ggo_donot_write_inctn_in_wcs_recover 720
# define ggt_donot_write_inctn_in_wcs_recover boolean_t
# define ggo_gbuff_limit 728
# define ggt_gbuff_limit mval
# define ggo_gd_targ_tn 760
# define ggt_gd_targ_tn trans_num
# define ggo_gd_targ_reg_array 768
# define ggt_gd_targ_reg_array trans_num *
# define ggo_gd_targ_reg_array_size 776
# define ggt_gd_targ_reg_array_size uint4
# define ggo_gd_targ_addr 784
# define ggt_gd_targ_addr gd_addr *
# define ggo_gd_targ_gvnh_reg 792
# define ggt_gd_targ_gvnh_reg gvnh_reg_t *
# define ggo_gd_targ_map 800
# define ggt_gd_targ_map gd_binding *
# define ggo_ydb_custom_errors 808
# define ggt_ydb_custom_errors mstr
# define ggo_gv_extname_size 824
# define ggt_gv_extname_size int4
# define ggo_gv_last_subsc_null 828
# define ggt_gv_last_subsc_null boolean_t
# define ggo_gv_mergekey2 832
# define ggt_gv_mergekey2 gv_key *
# define ggo_gv_reorgkey 840
# define ggt_gv_reorgkey gv_key *
# define ggo_gv_some_subsc_null 848
# define ggt_gv_some_subsc_null boolean_t
# define ggo_gv_sparekey 856
# define ggt_gv_sparekey gv_key *
# define ggo_gv_sparekey_mval 864
# define ggt_gv_sparekey_mval mval
# define ggo_gv_sparekey_size 896
# define ggt_gv_sparekey_size int4
# define ggo_gv_tporigkey_ptr 904
# define ggt_gv_tporigkey_ptr gv_orig_key_array *
# define ggo_gv_tporig_extnam_str 912
# define ggt_gv_tporig_extnam_str mstr
# define ggo_in_gvcst_bmp_mark_free 928
# define ggt_in_gvcst_bmp_mark_free boolean_t
# define ggo_in_gvcst_redo_root_search 932
# define ggt_in_gvcst_redo_root_search boolean_t
# define ggo_in_op_gvget 936
# define ggt_in_op_gvget boolean_t
# define ggo_issue_DBROLLEDBACK_anyways 940
# define ggt_issue_DBROLLEDBACK_anyways boolean_t
# define ggo_last_fnquery_return_subcnt 944
# define ggt_last_fnquery_return_subcnt int
# define ggo_last_fnquery_return_varname 952
# define ggt_last_fnquery_return_varname mval
# define ggo_nontprestart_count 984
# define ggt_nontprestart_count uint4
# define ggo_nontprestart_log_first 988
# define ggt_nontprestart_log_first int4
# define ggo_nontprestart_log_delta 992
# define ggt_nontprestart_log_delta int4
# define ggo_block_now_locked 1000
# define ggt_block_now_locked cache_rec_ptr_t
# define ggo_ok_to_call_wcs_recover 1008
# define ggt_ok_to_call_wcs_recover boolean_t
# define ggo_prev_gv_target 1016
# define ggt_prev_gv_target gv_namehead *
# define ggo_ready2signal_gvundef 1024
# define ggt_ready2signal_gvundef boolean_t
# define ggo_redo_rootsrch_ctxt 1032
# define ggt_redo_rootsrch_ctxt redo_root_search_context
# define ggo_semwait2long 2192
# define ggt_semwait2long volatile boolean_t
# define ggo_skip_file_corrupt_check 2196
# define ggt_skip_file_corrupt_check boolean_t
# define ggo_tpnotacidtime 2200
# define ggt_tpnotacidtime mval
# define ggo_tp_restart_count 2232
# define ggt_tp_restart_count uint4
# define ggo_tp_restart_dont_counts 2236
# define ggt_tp_restart_dont_counts int4
# define ggo_tp_restart_entryref 2240
# define ggt_tp_restart_entryref mval
# define ggo_tp_restart_failhist_indx 2272
# define ggt_tp_restart_failhist_indx int4
# define ggo_tprestart_syslog_delta 2276
# define ggt_tprestart_syslog_delta int4
# define ggo_tprestart_syslog_first 2280
# define ggt_tprestart_syslog_first int4
# define ggo_t_fail_hist_blk 2284
# define ggt_t_fail_hist_blk block_id
# define ggl_t_fail_hist_blk 20
# define ggo_tp_fail_bttn 2304
# define ggt_tp_fail_bttn trans_num
# define ggl_tp_fail_bttn 40
# define ggo_tp_fail_histtn 2344
# define ggt_tp_fail_histtn trans_num
# define ggl_tp_fail_histtn 40
# define ggo_tp_fail_hist 2384
# define ggt_tp_fail_hist gv_namehead *
# define ggl_tp_fail_hist 40
# define ggo_tp_fail_hist_reg 2424
# define ggt_tp_fail_hist_reg gd_region *
# define ggl_tp_fail_hist_reg 40
# define ggo_transform 2464
# define ggt_transform boolean_t
# define ggo_wcs_recover_done 2468
# define ggt_wcs_recover_done boolean_t
# define ggo_curr_symval_cycle 2472
# define ggt_curr_symval_cycle unsigned int
# define ggo_in_op_fnnext 2476
# define ggt_in_op_fnnext boolean_t
# define ggo_local_collseq 2480
# define ggt_local_collseq collseq *
# define ggo_local_collseq_stdnull 2488
# define ggt_local_collseq_stdnull boolean_t
# define ggo_local_coll_nums_as_strings 2492
# define ggt_local_coll_nums_as_strings boolean_t
# define ggo_lvmon_active 2496
# define ggt_lvmon_active boolean_t
# define ggo_lvmon_vars_anchor 2504
# define ggt_lvmon_vars_anchor lvmon_var *
# define ggo_lvmon_vars_count 2512
# define ggt_lvmon_vars_count int
# define ggo_lv_null_subs 2516
# define ggt_lv_null_subs int
# define ggo_max_lcl_coll_xform_bufsiz 2520
# define ggt_max_lcl_coll_xform_bufsiz int
# define ggo_replgbl 2528
# define ggt_replgbl replgbl_t
# define ggo_tqread_nowait 2552
# define ggt_tqread_nowait boolean_t
# define ggo_arlink_enabled 2556
# define ggt_arlink_enabled boolean_t
# define ggo_arlink_loaded 2560
# define ggt_arlink_loaded uint4
# define ggo_collseq_list 2568
# define ggt_collseq_list collseq *
# define ggo_create_fatal_error_zshow_dmp_fptr 2576
# define ggt_create_fatal_error_zshow_dmp_fptr void
# define gga_create_fatal_error_zshow_dmp_fptr (void)
typedef void (*ggf_create_fatal_error_zshow_dmp_fptr)(void);
# define ggo_curWorkQHead 2584
# define ggt_curWorkQHead stm_workq *
# define ggo_stapi_errstr 2592
# define ggt_stapi_errstr ydb_buffer_t *
# define ggo_disable_sigcont 2600
# define ggt_disable_sigcont boolean_t
# define ggo_dollar_zcompile 2608
# define ggt_dollar_zcompile mstr
# define ggo_dollar_etrap 2624
# define ggt_dollar_etrap mval
# define ggo_dollar_zmode 2656
# define ggt_dollar_zmode mval
# define ggo_dollar_zonlnrlbk 2688
# define ggt_dollar_zonlnrlbk int
# define ggo_dollar_zclose 2692
# define ggt_dollar_zclose int
# define ggo_dollar_zroutines 2696
# define ggt_dollar_zroutines mstr
# define ggo_dollar_zstep 2712
# define ggt_dollar_zstep mval
# define ggo_dollar_ztrap 2744
# define ggt_dollar_ztrap mval
# define ggo_error_on_jnl_file_lost 2776
# define ggt_error_on_jnl_file_lost unsigned int
# define ggo_fnzsearch_lv_vars 2784
# define ggt_fnzsearch_lv_vars lv_val *
# define ggo_fnzsearch_sub_mval 2792
# define ggt_fnzsearch_sub_mval mval
# define ggo_fnzsearch_nullsubs_sav 2824
# define ggt_fnzsearch_nullsubs_sav int
# define ggo_fnzsearch_globbuf_ptr 2832
# define ggt_fnzsearch_globbuf_ptr glob_t *
# define ggo_glvn_pool_ptr 2840
# define ggt_glvn_pool_ptr glvn_pool *
# define ggo_gtmci_retval 2848
# define ggt_gtmci_retval mval *
# define ggo_ydb_dbgflags 2856
# define ggt_ydb_dbgflags int
# define ggo_ydb_dbgflags_freq 2860
# define ggt_ydb_dbgflags_freq int
# define ggo_ydb_dbgflags_freq_cntr 2864
# define ggt_ydb_dbgflags_freq_cntr int
# define ggo_gtm_env_init_started 2868
# define ggt_gtm_env_init_started boolean_t
# define ggo_gtm_env_xlate_entry 2872
# define ggt_gtm_env_xlate_entry int
# define gga_gtm_env_xlate_entry ()
typedef int (*ggf_gtm_env_xlate_entry)();
# define ggo_ydb_environment_init 2880
# define ggt_ydb_environment_init boolean_t
# define ggo_gtm_sigusr1_handler 2888
# define ggt_gtm_sigusr1_handler void
# define gga_gtm_sigusr1_handler (void)
typedef void (*ggf_gtm_sigusr1_handler)(void);
# define ggo_ydb_linktmpdir 2896
# define ggt_ydb_linktmpdir mstr
# define ggo_gtm_strpllim 2912
# define ggt_gtm_strpllim int4
# define ggo_gtm_strpllimwarned 2916
# define ggt_gtm_strpllimwarned boolean_t
# define ggo_ydb_trigger_etrap 2920
# define ggt_ydb_trigger_etrap mval
# define ggo_gtm_trctbl_cur 2952
# define ggt_gtm_trctbl_cur trctbl_entry *
# define ggo_gtm_trctbl_end 2960
# define ggt_gtm_trctbl_end trctbl_entry *
# define ggo_gtm_trctbl_groups 2968
# define ggt_gtm_trctbl_groups unsigned int
# define ggo_gtm_trctbl_start 2976
# define ggt_gtm_trctbl_start trctbl_entry *
# define ggo_gtm_waitstuck_script 2984
# define ggt_gtm_waitstuck_script mstr
# define ggo_gtmprompt 3000
# define ggt_gtmprompt mstr
# define ggo_gtmsecshr_comkey 3016
# define ggt_gtmsecshr_comkey unsigned int
# define ggo_gvcst_statsDB_open_ch_active 3020
# define ggt_gvcst_statsDB_open_ch_active boolean_t
# define ggo_in_zwrite 3024
# define ggt_in_zwrite boolean_t
# define ggo_lab_lnr 3032
# define ggt_lab_lnr lnr_tabent **
# define ggo_jobexam_counter 3040
# define ggt_jobexam_counter unsigned int
# define ggo_lnk_proxy 3048
# define ggt_lnk_proxy lnk_tabent_proxy
# define ggo_libyottadb_active_rtn 3072
# define ggt_libyottadb_active_rtn libyottadb_routines
# define ggo_mprof_alloc_reclaim 3076
# define ggt_mprof_alloc_reclaim boolean_t
# define ggo_mprof_chunk_avail_size 3080
# define ggt_mprof_chunk_avail_size int
# define ggo_mprof_env_gbl_name 3088
# define ggt_mprof_env_gbl_name mval
# define ggo_mprof_ptr 3120
# define ggt_mprof_ptr mprof_wrapper *
# define ggo_mprof_reclaim_addr 3128
# define ggt_mprof_reclaim_addr char *
# define ggo_mprof_reclaim_cnt 3136
# define ggt_mprof_reclaim_cnt int
# define ggo_mprof_stack_curr_frame 3144
# define ggt_mprof_stack_curr_frame mprof_stack_frame *
# define ggo_mprof_stack_next_frame 3152
# define ggt_mprof_stack_next_frame mprof_stack_frame *
# define ggo_mu_cre_file_openrc 3160
# define ggt_mu_cre_file_openrc int
# define ggo_open_relinkctl_list 3168
# define ggt_open_relinkctl_list open_relinkctl_sgm *
# define ggo_relinkctl_shm_min_index 3176
# define ggt_relinkctl_shm_min_index int
# define ggo_ydb_autorelink_keeprtn 3180
# define ggt_ydb_autorelink_keeprtn boolean_t
# define ggo_open_shlib_root 3184
# define ggt_open_shlib_root open_shlib *
# define ggo_parm_pool_ptr 3192
# define ggt_parm_pool_ptr parm_pool *
# define ggo_parms_cnt 3200
# define ggt_parms_cnt unsigned int
# define ggo_rts_error_depth 3204
# define ggt_rts_error_depth unsigned int
# define ggo_sapi_query_node_subs 3208
# define ggt_sapi_query_node_subs mstr *
# define ggo_sapi_query_node_subs_cnt 3216
# define ggt_sapi_query_node_subs_cnt int
# define ggo_statsdb_fnerr_reason 3220
# define ggt_statsdb_fnerr_reason int
# define ggo_zpeek_regname 3224
# define ggt_zpeek_regname char
# define ggl_zpeek_regname 31
# define ggo_zpeek_regname_len 3256
# define ggt_zpeek_regname_len int
# define ggo_zpeek_reg_ptr 3264
# define ggt_zpeek_reg_ptr gd_region *
# define ggo_pipefifo_interrupt 3272
# define ggt_pipefifo_interrupt int
# define ggo_prof_fp 3280
# define ggt_prof_fp mprof_stack_frame *
# define ggo_relink_allowed 3288
# define ggt_relink_allowed int
# define ggo_save_zhist 3296
# define ggt_save_zhist zro_hist *
# define ggo_sapi_mstrs_for_gc_ary 3304
# define ggt_sapi_mstrs_for_gc_ary mstr *
# define ggl_sapi_mstrs_for_gc_ary 288
# define ggo_sapi_mstrs_for_gc_indx 3592
# define ggt_sapi_mstrs_for_gc_indx int
# define ggo_set_zroutines_cycle 3596
# define ggt_set_zroutines_cycle uint4
# define ggo_statsDB_init_defer_anchor 3600
# define ggt_statsDB_init_defer_anchor statsDB_deferred_init_que_elem *
# define ggo_statshare_opted_in 3608
# define ggt_statshare_opted_in uint4
# define ggo_trans_code_pop 3616
# define ggt_trans_code_pop mval *
# define ggo_view_ydirt_str 3624
# define ggt_view_ydirt_str char *
# define ggo_view_ydirt_str_len 3632
# define ggt_view_ydirt_str_len int4
# define ggo_view_region_list 3640
# define ggt_view_region_list tp_region *
# define ggo_view_region_free_list 3648
# define ggt_view_region_free_list tp_region *
# define ggo_ydb_error_code 3656
# define ggt_ydb_error_code int
# define ggo_zdate_form 3660
# define ggt_zdate_form int4
# define ggo_zintcmd_active 3664
# define ggt_zintcmd_active zintcmd_active_info
# define ggl_zintcmd_active 72
# define ggo_zro_root 3736
# define ggt_zro_root zro_ent *
# define ggo_zsearch_var 3744
# define ggt_zsearch_var lv_val *
# define ggo_ztrap_form 3752
# define ggt_ztrap_form int4
# define ggo_poll_fds_buffer 3760
# define ggt_poll_fds_buffer char *
# define ggo_poll_fds_buffer_size 3768
# define ggt_poll_fds_buffer_size size_t
# define ggo_socket_handle_counter 3776
# define ggt_socket_handle_counter int
# define ggo_mu_set_file_noencryptable 3780
# define ggt_mu_set_file_noencryptable boolean_t
# define ggo_director_string 3784
# define ggt_director_string char
# define ggl_director_string 64
# define ggo_fnpca 3848
# define ggt_fnpca fnpc_area
# define ggo_for_stack 21864
# define ggt_for_stack oprtype *
# define ggl_for_stack 256
# define ggo_for_temps 22120
# define ggt_for_temps boolean_t
# define ggl_for_temps 128
# define ggo_ydb_utfcgr_strings 22248
# define ggt_ydb_utfcgr_strings int
# define ggo_ydb_utfcgr_string_groups 22252
# define ggt_ydb_utfcgr_string_groups int
# define ggo_last_fnquery_return_sub 22256
# define ggt_last_fnquery_return_sub mval
# define ggl_last_fnquery_return_sub 1024
# define ggo_lcl_coll_xform_buff 23280
# define ggt_lcl_coll_xform_buff char *
# define ggo_protmem_ba 23288
# define ggt_protmem_ba mstr
# define ggo_parm_ary 23304
# define ggt_parm_ary char *
# define ggl_parm_ary 8192
# define ggo_parm_ary_len 31496
# define ggt_parm_ary_len int
# define ggl_parm_ary_len 4096
# define ggo_parm_str_len 35592
# define ggt_parm_str_len int
# define ggl_parm_str_len 4096
# define ggo_prombuf 39688
# define ggt_prombuf char
# define ggl_prombuf 32
# define ggo_tmp_object_file_name 39720
# define ggt_tmp_object_file_name char
# define ggl_tmp_object_file_name 4097
# define ggo_tp_restart_failhist_arry 43817
# define ggt_tp_restart_failhist_arry char
# define ggl_tp_restart_failhist_arry 32
# define ggo_utfcgra 43856
# define ggt_utfcgra utfcgr_area
# define ggo_utfcgr_string_lookmax 43888
# define ggt_utfcgr_string_lookmax int
# define ggo_window_string 43892
# define ggt_window_string char
# define ggl_window_string 32
# define ggo_last_va_list_ptr 43928
# define ggt_last_va_list_ptr va_list
# define ggo_util_outbuff 43952
# define ggt_util_outbuff char
# define ggl_util_outbuff 6144
# define ggo_util_outbuff_ptr 50096
# define ggt_util_outbuff_ptr char *
# define ggo_util_outptr 50104
# define ggt_util_outptr char *
# define ggo_callin_hashtab 50112
# define ggt_callin_hashtab hash_table_str *
# define ggo_ci_table 50120
# define ggt_ci_table callin_entry_list *
# define ggo_ci_filter_hashtab 50128
# define ggt_ci_filter_hashtab hash_table_str *
# define ggo_ci_filter_table 50136
# define ggt_ci_filter_table callin_entry_list *
# define ggo_extcall_package_root 50144
# define ggt_extcall_package_root struct extcall_package_list *
# define ggo_gtmci_nested_level 50152
# define ggt_gtmci_nested_level unsigned int
# define ggo_comm_filter_init 50156
# define ggt_comm_filter_init boolean_t
# define ggo_temp_fgncal_stack 50160
# define ggt_temp_fgncal_stack unsigned char *
# define ggo_midchild_send_locals 50168
# define ggt_midchild_send_locals boolean_t
# define ggo_want_empty_gvts 50172
# define ggt_want_empty_gvts boolean_t
# define ggo_in_mu_swap_root_state 50176
# define ggt_in_mu_swap_root_state unsigned int
# define ggo_prev_t_tries 50180
# define ggt_prev_t_tries unsigned int
# define ggo_rlbk_during_redo_root 50184
# define ggt_rlbk_during_redo_root boolean_t
# define ggo_mlk_yield_pid 50188
# define ggt_mlk_yield_pid uint4
# define ggo_jnl_extract_nocol 50192
# define ggt_jnl_extract_nocol uint4
# define ggo_skip_gtm_putmsg 50196
# define ggt_skip_gtm_putmsg boolean_t
# define ggo_spangbl_seen 50200
# define ggt_spangbl_seen boolean_t
# define ggo_no_spangbls 50204
# define ggt_no_spangbls boolean_t
# define ggo_max_fid_index 50208
# define ggt_max_fid_index int
# define ggo_is_mu_rndwn_rlnkctl 50212
# define ggt_is_mu_rndwn_rlnkctl int
# define ggo_expand_prev_key 50216
# define ggt_expand_prev_key int
# define ggo_ydb_autorelink_ctlmax 50220
# define ggt_ydb_autorelink_ctlmax uint4
# define ggo_dio_buff 50224
# define ggt_dio_buff dio_buff_t
# define ggo_gvt_triggers_read_this_tn 50248
# define ggt_gvt_triggers_read_this_tn boolean_t
# define ggo_op_fntext_tlevel 50252
# define ggt_op_fntext_tlevel uint4
# define ggo_in_op_fntext 50256
# define ggt_in_op_fntext boolean_t
# define ggo_ztrigbuff 50264
# define ggt_ztrigbuff char *
# define ggo_ztrigbuffAllocLen 50272
# define ggt_ztrigbuffAllocLen int
# define ggo_ztrigbuffLen 50276
# define ggt_ztrigbuffLen int
# define ggo_ztrig_use_io_curr_device 50280
# define ggt_ztrig_use_io_curr_device boolean_t
# define ggo_in_ext_call 50284
# define ggt_in_ext_call boolean_t
# define ggo_ydb_aio_nr_events 50288
# define ggt_ydb_aio_nr_events uint4
# define ggo_crit_reg_count 50292
# define ggt_crit_reg_count int4
# define ggo_ok_to_see_statsdb_regs 50296
# define ggt_ok_to_see_statsdb_regs boolean_t
# define ggo_was_open_reg_seen 50300
# define ggt_was_open_reg_seen boolean_t
# define ggo_nontp_jbuf_rsrv 50304
# define ggt_nontp_jbuf_rsrv jbuf_rsrv_struct_t *
# define ggo_last_gvquery_key 50312
# define ggt_last_gvquery_key gv_key *
# define ggo_ydbmsgprefixbuf 50320
# define ggt_ydbmsgprefixbuf char
# define ggl_ydbmsgprefixbuf 32
# define ggo_ydbmsgprefix 50352
# define ggt_ydbmsgprefix mstr
# define ggo_trig_forced_unwind 50368
# define ggt_trig_forced_unwind boolean_t
# define ggo_LengthReentCnt 50372
# define ggt_LengthReentCnt boolean_t
# define ggo_ZLengthReentCnt 50376
# define ggt_ZLengthReentCnt boolean_t
# define ggo_donot_commit 50380
# define ggt_donot_commit boolean_t
# define ggo_continue_proc_cnt 50384
# define ggt_continue_proc_cnt int
# define ggo_ydb_test_fake_enospc 50388
# define ggt_ydb_test_fake_enospc boolean_t
# define ggo_ydb_test_jnlpool_sync 50392
# define ggt_ydb_test_jnlpool_sync uint4
# define ggo_ydb_usesecshr 50396
# define ggt_ydb_usesecshr boolean_t
# define ggo_rts_error_unusable 50400
# define ggt_rts_error_unusable boolean_t
# define ggo_rts_error_unusable_seen 50404
# define ggt_rts_error_unusable_seen boolean_t
# define ggo_trans_restart_hist_array 50408
# define ggt_trans_restart_hist_array trans_restart_hist_t
# define ggl_trans_restart_hist_array 20480
# define ggo_trans_restart_hist_index 70888
# define ggt_trans_restart_hist_index uint4
# define ggo_skip_mv_num_approx_assert 70892
# define ggt_skip_mv_num_approx_assert boolean_t
# define ggo_ydb_gvundef_fatal 70896
# define ggt_ydb_gvundef_fatal boolean_t
# define ggo_ydb_lockhash_n_bits 70900
# define ggt_ydb_lockhash_n_bits uint4
# define ggo_ydb_dirtree_collhdr_always 70904
# define ggt_ydb_dirtree_collhdr_always boolean_t
# define ggo_activelv_cycle 70908
# define ggt_activelv_cycle int
# define ggo_activelv_index 70912
# define ggt_activelv_index int
# define ggo_activelv_dbg_array 70920
# define ggt_activelv_dbg_array activelv_dbg_t *
# define ggo_cli_get_str_max_len 70928
# define ggt_cli_get_str_max_len uint4
# define ggo_gtmio_skip_tlevel_assert 70932
# define ggt_gtmio_skip_tlevel_assert boolean_t
# define ggo_in_trigger_upgrade 70936
# define ggt_in_trigger_upgrade boolean_t
# define ggo_ydb_test_autorelink_always 70940
# define ggt_ydb_test_autorelink_always boolean_t
# define ggo_fork_without_child_wait 70944
# define ggt_fork_without_child_wait boolean_t
# define size_gtm_threadgbl_struct 70952
