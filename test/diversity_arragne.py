# coding=utf8
import random
import math

color = ["1", "2", "3", "4", "5"]
category = ["dog", "beauty", "building", "dress", "photograph"]


def generate_candidates(num=50):
    sample_arr = []
    for i in range(num):
        color_index = random.randint(0, len(color) - 1)
        category_index = random.randint(0, len(category) - 1)
        sample_arr.append((color[color_index], category[category_index]))
    for i in range(8):
        sample_arr.append(("3", "dress"))
        sample_arr.append(("4", "dress"))
    return sample_arr


def get_key(item):
    return item[0]


def get_category(item):
    return item[1]


def get_sorted_arrs(elem_arr):
    ordered_one_arr = sorted(elem_arr, key=get_key)
    all_color_arrs = []
    temp_color = None
    same_color_arr = []
    for color_category_pair in ordered_one_arr:
        current_color = get_key(color_category_pair)
        if temp_color != current_color:
            if temp_color and len(same_color_arr) > 0:
                all_color_arrs.append(same_color_arr)
            temp_color = current_color
            same_color_arr = []
        same_color_arr.append(color_category_pair)
    if temp_color and len(same_color_arr) > 0:
        all_color_arrs.append(same_color_arr)
    return all_color_arrs


# def minimize_max_block(elem_list, last_block_num, category_info, threshold=8):
#     list_info = get_category_list(elem_list)


def construct_arr_info(input_arr):
    sort_color_arrs = get_sorted_arrs(input_arr)
    pre_cate = None
    pre_num = 0
    for sort_color_arr in sort_color_arrs:
        category_in_color_arrs = get_category_list(sort_color_arr)
        permute_in_category_list(category_in_color_arrs, 8, pre_cate, pre_num)



def permute_in_category_list(category_lists, threshold, pre_cate=None, pre_num=0):
    if len(category_lists) < 2:
        return category_lists[0]

    cate_index_map, key_list = get_permute_info_in_category_list(category_lists)
    cate_index_map, key_list, arranged_result = preprocess_arrange(category_lists, cate_index_map,
                                                                   key_list, threshold, pre_cate, pre_num)
    max_average = get_permute_key_split(key_list)
    now_cate, now_num = range_impl(category_lists, max_average, key_list, cate_index_map, arranged_result)
    return arranged_result, now_cate, now_num


def range_impl(category_lists, max_average, key_list, cate_index_map, arranged_result):
    offset_arr = [0] * len(category_lists)
    not_completed = True
    last_cate = None
    last_size = 0
    is_need_sep = False
    while not_completed:
        not_completed = False
        max_cate = key_list[0][0]
        not_completed_one_list, move_size = copy_value_by_step(max_average, offset_arr, category_lists,
                                                               max_cate, cate_index_map, arranged_result)
        if not_completed_one_list:
            last_cate = max_cate
            last_size = move_size
            not_completed = True
            is_need_sep = True
        for i in range(1, key_list):
            cate = key_list[i][0]
            current_index = cate_index_map[cate]
            if is_need_sep:
                not_completed_one_list, move_size = copy_value_by_step(1,
                                                                       offset_arr,
                                                                       category_lists,
                                                                       max_cate,
                                                                       cate_index_map,
                                                                       arranged_result)
            else:
                max_cate = key_list[0][0]
                max_index = cate_index_map[max_cate]
                max_need_step = (len(category_lists[max_index]) - offset_arr[max_index] + max_average - 1)/max_average
                current_need_step = len(category_lists[max_index]) - offset_arr[current_index]
                if max_need_step < current_need_step:
                    not_completed_one_list, move_size = copy_value_by_step(1,
                                                                           offset_arr,
                                                                           category_lists,
                                                                           max_cate,
                                                                           cate_index_map,
                                                                           arranged_result)
                else:
                    not_completed_one_list = False

            if not_completed_one_list:
                last_cate = cate
                last_size = move_size
                is_need_sep = False
                not_completed = True
    return last_cate, last_size


def copy_value_by_step(step, offset_arr, category_lists, cate, cate_index_map, result_list):
    list_index = cate_index_map[cate]
    the_list = category_lists[list_index]
    if len(the_list) > offset_arr[list_index]:
        slice_length = min(offset_arr[list_index]+step, len(the_list))
        move_size = slice_length - offset_arr[list_index]
        result_list.extend(the_list[offset_arr[list_index]:slice_length])
        offset_arr[list_index] = slice_length
        return True, move_size
    return False, 0


def preprocess_arrange(category_lists, cate_index_map, key_list, threshold, pre_cate=None, pre_num=0):
    arranged_result = []
    if pre_cate is None :
        return cate_index_map, key_list, arranged_result
    if pre_cate != get_key(key_list[0]):
        return cate_index_map, key_list, arranged_result
    max_need_num = min(threshold - pre_num, key_list[0][1])
    max_index = cate_index_map[key_list[0][0]]
    second_index = cate_index_map[key_list[1][0]]
    if max_need_num > 0:
        arranged_result.extend(category_lists[max_index][0:max_need_num])
        category_lists[max_index] = category_lists[max_index][max_need_num:key_list[0][1]]
        key_list[0][1] -= max_need_num
    arranged_result.append(category_lists[second_index][0])
    category_lists[second_index] = category_lists[second_index][1: key_list[1][1]]
    key_list[1][1] -= 1
    return cate_index_map, key_list, arranged_result


def get_permute_key_split(key_list):
    if len(key_list) < 2:
        return key_list[0][1]
    sum_value = 0
    for i in range(1, len(key_list)):
        sum_value += key_list[i][1]
    max_average = int(math.ceil(float(key_list[0][1]) / (sum + 1)))
    return max_average


def get_permute_info_in_category_list(category_lists):
    cate_index_map = {}
    cate_num_items = []
    for index, cate_list in enumerate(category_lists):
        cate = get_category(cate_list[0])
        cate_index_map[cate] = index
        cate_num_items.append([cate, len(cate_list)])
    print cate_index_map
    return cate_index_map, sorted(cate_num_items, key=get_rate)


def get_rate(item):
    return 0 - int(item[1])


def get_category_list(elem_list):
    ordered_list = sorted(elem_list, key=get_category)
    result_arrs = []
    temp_color = None
    same_color_arr = []
    for color_category_pair in ordered_list:
        current_color = get_category(color_category_pair)
        if temp_color != current_color:
            if temp_color and len(same_color_arr) > 0:
                result_arrs.append(same_color_arr)
            temp_color = current_color
            same_color_arr = []
        same_color_arr.append(color_category_pair)
    if temp_color and len(same_color_arr) > 0:
        result_arrs.append(same_color_arr)
    return result_arrs


if __name__ == '__main__':
    arr_elems = generate_candidates()

    construct_arr_info(arr_elems)